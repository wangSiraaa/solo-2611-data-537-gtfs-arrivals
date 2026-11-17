"""Live arrivals warehouse over a fixed published scheduled-calls catalog.

The warehouse binds itself, at construction time, to one on-disk
``scheduled-calls/v1`` directory (produced by :class:`TimetableService` or any
other publisher obeying the same row format and ordering rules). It never holds
a reference to a publisher object or private callback: the catalog is read
through :class:`~timetable.CatalogReader`, and a snapshot of it is materialized
into SQLite so that reopening the database re-explains every active entity and
prediction from durable state alone.

Feed handling is strict and serial:

* only the documented GTFS-RT v2.0 subset is accepted; every other known field
  (``route_id``, ``direction_id``, ``TripUpdate.delay``,
  ``StopTimeEvent.uncertainty``, ``vehicle`` ...) and every unknown wire field
  rejects the whole feed instead of being silently ignored;
* a feed is validated end to end before any write, so a bad last entity cannot
  leave half of its predictions behind;
* timestamps move strictly forward, equal-timestamp replay is byte-idempotent;
* FULL_DATASET replaces the whole active set, DIFFERENTIAL only upserts/deletes
  the entities it names; entity deletion or re-pointing restores the old
  instance to its static, prediction-free state.

Each predicted arrival/departure keeps its *basis* (an explicit feed value vs.
propagation from an earlier stop's event) and the exact source stop/event, so
NO_DATA cuts, SKIPPED carry-over and later re-establishment are all auditable.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from google.protobuf import unknown_fields
from google.transit import gtfs_realtime_pb2 as gtfs

from timetable import CatalogReader, TimetableError

GTFS_VERSION = "2.0"
MAX_DELAY_ABS = 7200  # documented input envelope; used only for sanity checks

# Explicit field-name allow-lists. Any other *known* descriptor field appearing
# on the wire is part of the GTFS-RT schema but explicitly unsupported here.
HEADER_FIELDS = {"gtfs_realtime_version", "incrementality", "timestamp"}
ENTITY_FIELDS = {"id", "is_deleted", "trip_update"}
TRIP_UPDATE_FIELDS = {"trip", "stop_time_update"}
TRIP_DESCRIPTOR_FIELDS = {
    "trip_id", "start_date", "start_time", "schedule_relationship",
}
STU_FIELDS = {
    "stop_sequence", "stop_id", "arrival", "departure",
    "schedule_relationship",
}
EVENT_FIELDS = {"time", "delay"}

ALLOWED_INCREMENTALITY = {
    gtfs.FeedHeader.FULL_DATASET: "FULL_DATASET",
    gtfs.FeedHeader.DIFFERENTIAL: "DIFFERENTIAL",
}
ALLOWED_TRIP_RELATIONSHIPS = {
    gtfs.TripDescriptor.SCHEDULED: "SCHEDULED",
    gtfs.TripDescriptor.CANCELED: "CANCELED",
}
ALLOWED_STOP_RELATIONSHIPS = {
    gtfs.TripUpdate.StopTimeUpdate.SCHEDULED: "SCHEDULED",
    gtfs.TripUpdate.StopTimeUpdate.SKIPPED: "SKIPPED",
    gtfs.TripUpdate.StopTimeUpdate.NO_DATA: "NO_DATA",
}


class LiveFeedError(ValueError):
    """A feed violates the supported subset or the timestamp contract."""


def _present_fields(message):
    """Names of set fields plus numbers of truly unknown wire fields."""
    names = {field.name for field, _ in message.ListFields()}
    unknown = {item.field_number for item in unknown_fields.UnknownFieldSet(message)}
    return names, unknown


def _check_fields(message, allowed, label):
    names, unknown = _present_fields(message)
    extra = names - allowed
    if extra:
        raise LiveFeedError(f"{label}: unsupported field(s): {sorted(extra)}")
    if unknown:
        raise LiveFeedError(
            f"{label}: unknown wire field number(s): {sorted(unknown)}"
        )


# --------------------------------------------------------------------------- #
# Feed validation
# --------------------------------------------------------------------------- #
def _validate_event(event, label):
    _check_fields(event, EVENT_FIELDS, label)
    for name in ("time", "delay"):
        if event.HasField(name):
            value = getattr(event, name)
            if not isinstance(value, int):
                raise LiveFeedError(f"{label}.{name} must be an integer")
    if event.HasField("delay") and abs(event.delay) > MAX_DELAY_ABS:
        raise LiveFeedError(f"{label}.delay outside +/-{MAX_DELAY_ABS}s envelope")


def _validate_stu(stu, label):
    _check_fields(stu, STU_FIELDS, label)
    if stu.HasField("schedule_relationship"):
        if stu.schedule_relationship not in ALLOWED_STOP_RELATIONSHIPS:
            raise LiveFeedError(
                f"{label}: unsupported stop schedule_relationship "
                f"{stu.schedule_relationship}"
            )
        relationship = ALLOWED_STOP_RELATIONSHIPS[stu.schedule_relationship]
    else:
        relationship = "SCHEDULED"
    if relationship in ("SKIPPED", "NO_DATA"):
        if stu.HasField("arrival") or stu.HasField("departure"):
            raise LiveFeedError(
                f"{label}: {relationship} stop must not carry arrival/departure"
            )
        return relationship
    for event_name in ("arrival", "departure"):
        if stu.HasField(event_name):
            _validate_event(
                getattr(stu, event_name), f"{label}.{event_name}"
            )
    return relationship


def _validate_trip_descriptor(trip, label):
    _check_fields(trip, TRIP_DESCRIPTOR_FIELDS, label)
    for name in ("trip_id", "start_date", "start_time"):
        if not trip.HasField(name) or getattr(trip, name) == "":
            raise LiveFeedError(f"{label}: missing instance key field {name}")
    if trip.HasField("schedule_relationship"):
        if trip.schedule_relationship not in ALLOWED_TRIP_RELATIONSHIPS:
            raise LiveFeedError(
                f"{label}: unsupported trip schedule_relationship "
                f"{trip.schedule_relationship}"
            )
        return ALLOWED_TRIP_RELATIONSHIPS[trip.schedule_relationship]
    return "SCHEDULED"


def _validate_feed(feed, *, allow_deletes):
    """Structural validation that needs no catalog; returns header summary."""
    _check_fields(feed, {"header", "entity"}, "FeedMessage")
    _check_fields(feed.header, HEADER_FIELDS, "FeedHeader")
    header = feed.header
    if not header.HasField("gtfs_realtime_version"):
        raise LiveFeedError("FeedHeader.gtfs_realtime_version is required")
    if header.gtfs_realtime_version != GTFS_VERSION:
        raise LiveFeedError(
            f"unsupported gtfs_realtime_version {header.gtfs_realtime_version!r}, "
            f"want {GTFS_VERSION!r}"
        )
    if not header.HasField("timestamp") or header.timestamp <= 0:
        raise LiveFeedError("FeedHeader.timestamp must be a positive integer")
    if not header.HasField("incrementality"):
        raise LiveFeedError("FeedHeader.incrementality is required")
    if header.incrementality not in ALLOWED_INCREMENTALITY:
        raise LiveFeedError(
            f"unsupported incrementality {header.incrementality}"
        )
    incrementality = ALLOWED_INCREMENTALITY[header.incrementality]

    seen_ids = set()
    for index, entity in enumerate(feed.entity):
        label = f"entity[{index}]"
        _check_fields(entity, ENTITY_FIELDS, label)
        if not entity.id:
            raise LiveFeedError(f"{label}: entity id must be non-empty")
        if entity.id in seen_ids:
            raise LiveFeedError(f"{label}: duplicate entity id {entity.id!r}")
        seen_ids.add(entity.id)
        if entity.is_deleted:
            if not allow_deletes:
                raise LiveFeedError(
                    f"{label}: is_deleted is only valid in DIFFERENTIAL feeds"
                )
            if entity.HasField("trip_update"):
                raise LiveFeedError(
                    f"{label}: deleted entity must not carry a trip_update"
                )
            continue
        if not entity.HasField("trip_update"):
            raise LiveFeedError(
                f"{label}: non-deleted entity must contain a trip_update "
                "(vehicle/alert messages are not supported)"
            )
        update = entity.trip_update
        _check_fields(update, TRIP_UPDATE_FIELDS, f"{label}.trip_update")
        if not update.HasField("trip"):
            raise LiveFeedError(f"{label}.trip_update: trip descriptor required")
        _validate_trip_descriptor(update.trip, f"{label}.trip_update.trip")
        for stu_index, stu in enumerate(update.stop_time_update):
            _validate_stu(stu, f"{label}.stop_time_update[{stu_index}]")
    return incrementality


# --------------------------------------------------------------------------- #
# Projection: feed updates -> per-stop predictions with full provenance
# --------------------------------------------------------------------------- #
def _resolve_stops(update, calls, label):
    """Map each StopTimeUpdate to exactly one catalog call, feed order kept."""
    by_sequence = {call["stop_sequence"]: call for call in calls}
    by_stop_id = {}
    for call in calls:
        by_stop_id.setdefault(call["stop_id"], []).append(call)

    resolved = []
    last_sequence = None
    for stu_index, stu in enumerate(update.stop_time_update):
        stu_label = f"{label}.stop_time_update[{stu_index}]"
        if stu.HasField("stop_sequence"):
            sequence = stu.stop_sequence
            if sequence not in by_sequence:
                raise LiveFeedError(
                    f"{stu_label}: unknown stop_sequence {sequence} for instance"
                )
            call = by_sequence[sequence]
            if stu.HasField("stop_id") and stu.stop_id != call["stop_id"]:
                raise LiveFeedError(
                    f"{stu_label}: stop_id {stu.stop_id!r} contradicts "
                    f"stop_sequence {sequence} (catalog has {call['stop_id']!r})"
                )
        else:
            if not stu.HasField("stop_id"):
                raise LiveFeedError(
                    f"{stu_label}: update needs stop_sequence or stop_id"
                )
            matches = by_stop_id.get(stu.stop_id, [])
            if len(matches) != 1:
                raise LiveFeedError(
                    f"{stu_label}: stop_id {stu.stop_id!r} is not unique on this "
                    f"instance ({len(matches)} visits); use stop_sequence"
                )
            call = matches[0]
            sequence = call["stop_sequence"]
        if last_sequence is not None and sequence <= last_sequence:
            raise LiveFeedError(
                f"{stu_label}: stop_sequence values must be strictly increasing"
            )
        last_sequence = sequence
        relationship = (
            ALLOWED_STOP_RELATIONSHIPS[stu.schedule_relationship]
            if stu.HasField("schedule_relationship") else "SCHEDULED"
        )
        resolved.append((sequence, call, stu, relationship))
    return resolved


def _apply_event(event, scheduled, state, sequence, event_kind):
    """Return (predicted_value, basis, source_sequence, source_event).

    Also mutates the carried delay state when an explicit value is present.
    ``time`` wins over ``delay`` and re-bases the carried delay relative to the
    scheduled instant of the field it appears on.
    """
    if event is not None and event.HasField("time"):
        predicted = event.time
        state["delay"] = event.time - scheduled
        state["src_sequence"] = sequence
        state["src_event"] = event_kind
        return predicted, "explicit", sequence, event_kind
    if event is not None and event.HasField("delay"):
        predicted = scheduled + event.delay
        state["delay"] = event.delay
        state["src_sequence"] = sequence
        state["src_event"] = event_kind
        return predicted, "explicit", sequence, event_kind
    if state["delay"] is not None:
        return (
            scheduled + state["delay"],
            "propagated",
            state["src_sequence"],
            state["src_event"],
        )
    return None, None, None, None


def _project_scheduled(calls, update, label):
    """Walk the instance from its first stop, carrying one trusted delay."""
    resolved = _resolve_stops(update, calls, label)
    updates = {sequence: (stu, relationship)
               for sequence, _call, stu, relationship in resolved}

    state = {"delay": None, "src_sequence": None, "src_event": None}
    rows = []
    for call in calls:
        sequence = call["stop_sequence"]
        base = {
            "trip_id": call["trip_id"],
            "start_date": call["start_date"],
            "start_time": call["start_time"],
            "stop_sequence": sequence,
            "stop_id": call["stop_id"],
            "scheduled_arrival": call["arrival"],
            "scheduled_departure": call["departure"],
            "source_row": call["source_row"],
        }
        item = updates.get(sequence)
        if item is not None:
            _stu, relationship = item
        else:
            relationship = "SCHEDULED"

        record = dict(base)
        record.update({
            "predicted_arrival": None, "predicted_departure": None,
            "arrival_basis": None, "departure_basis": None,
            "arrival_src_sequence": None, "departure_src_sequence": None,
            "arrival_src_event": None, "departure_src_event": None,
            "null_reason": None, "stop_status": relationship.lower(),
        })

        if relationship == "SKIPPED":
            # No events allowed (validated upstream); the carried delay survives
            # and remains available to every later stop.
            record["null_reason"] = "skipped"
        elif relationship == "NO_DATA":
            # Explicit trust cut: predictions stay null until a later explicit
            # time/delay re-establishes a delay.
            state["delay"] = None
            state["src_sequence"] = None
            state["src_event"] = None
            record["null_reason"] = "no_data"
        else:
            record["stop_status"] = "scheduled"
            stu = item[0] if item is not None else None
            arrival_event = stu.arrival if stu is not None and stu.HasField("arrival") else None
            (record["predicted_arrival"], record["arrival_basis"],
             record["arrival_src_sequence"], record["arrival_src_event"]) = _apply_event(
                arrival_event, call["arrival"], state, sequence, "arrival"
            )
            departure_event = (
                stu.departure if stu is not None and stu.HasField("departure") else None
            )
            (record["predicted_departure"], record["departure_basis"],
             record["departure_src_sequence"], record["departure_src_event"]) = _apply_event(
                departure_event, call["departure"], state, sequence, "departure"
            )
            # Per-field basis columns already explain each side; the row reason
            # is only set when the whole stop lacks any live prediction.
            if (
                record["predicted_arrival"] is None
                and record["predicted_departure"] is None
            ):
                record["null_reason"] = "no_trust"
        rows.append(record)
    return rows


def _project_canceled(calls):
    rows = []
    for call in calls:
        rows.append({
            "trip_id": call["trip_id"],
            "start_date": call["start_date"],
            "start_time": call["start_time"],
            "stop_sequence": call["stop_sequence"],
            "stop_id": call["stop_id"],
            "scheduled_arrival": call["arrival"],
            "scheduled_departure": call["departure"],
            "source_row": call["source_row"],
            "predicted_arrival": None,
            "predicted_departure": None,
            "arrival_basis": None,
            "departure_basis": None,
            "arrival_src_sequence": None,
            "departure_src_sequence": None,
            "arrival_src_event": None,
            "departure_src_event": None,
            "null_reason": "canceled",
            "stop_status": "canceled",
        })
    return rows


# --------------------------------------------------------------------------- #
# Warehouse
# --------------------------------------------------------------------------- #
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS catalog_binding(
  id INTEGER PRIMARY KEY CHECK (id = 1),
  catalog_path TEXT NOT NULL,
  catalog_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS feed_state(
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_timestamp INTEGER NOT NULL,
  last_payload BLOB NOT NULL,
  incrementality TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS feed_log(
  feed_timestamp INTEGER NOT NULL,
  payload BLOB NOT NULL,
  incrementality TEXT NOT NULL,
  entity_count INTEGER NOT NULL,
  payload_sha256 TEXT NOT NULL,
  applied_at TEXT NOT NULL,
  PRIMARY KEY (feed_timestamp, payload_sha256)
);
CREATE TABLE IF NOT EXISTS active_entities(
  entity_id TEXT PRIMARY KEY,
  payload BLOB NOT NULL,
  trip_id TEXT NOT NULL,
  start_date TEXT NOT NULL,
  start_time TEXT NOT NULL,
  schedule_relationship TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS static_calls(
  trip_id TEXT NOT NULL,
  start_date TEXT NOT NULL,
  start_time TEXT NOT NULL,
  stop_sequence INTEGER NOT NULL,
  stop_id TEXT NOT NULL,
  scheduled_arrival INTEGER NOT NULL,
  scheduled_departure INTEGER NOT NULL,
  source_row INTEGER NOT NULL,
  PRIMARY KEY (trip_id, start_date, start_time, stop_sequence)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS predictions(
  trip_id TEXT NOT NULL,
  start_date TEXT NOT NULL,
  start_time TEXT NOT NULL,
  stop_sequence INTEGER NOT NULL,
  stop_id TEXT NOT NULL,
  scheduled_arrival INTEGER NOT NULL,
  scheduled_departure INTEGER NOT NULL,
  source_row INTEGER NOT NULL,
  predicted_arrival INTEGER,
  predicted_departure INTEGER,
  arrival_basis TEXT,
  departure_basis TEXT,
  arrival_src_sequence INTEGER,
  departure_src_sequence INTEGER,
  arrival_src_event TEXT,
  departure_src_event TEXT,
  null_reason TEXT,
  stop_status TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  PRIMARY KEY (trip_id, start_date, start_time, stop_sequence)
) WITHOUT ROWID;
CREATE VIEW IF NOT EXISTS arrivals AS
SELECT
  s.trip_id, s.start_date, s.start_time, s.stop_sequence, s.stop_id,
  s.scheduled_arrival, s.scheduled_departure, s.source_row,
  p.predicted_arrival, p.predicted_departure,
  p.arrival_basis, p.departure_basis,
  p.arrival_src_sequence, p.departure_src_sequence,
  p.arrival_src_event, p.departure_src_event,
  CASE
    WHEN p.entity_id IS NULL THEN 'no_realtime'
    WHEN p.null_reason IS NOT NULL THEN p.null_reason
    ELSE 'predicted'
  END AS null_reason,
  COALESCE(p.stop_status, 'scheduled') AS stop_status,
  p.entity_id
FROM static_calls s
LEFT JOIN predictions p
  ON p.trip_id = s.trip_id
 AND p.start_date = s.start_date
 AND p.start_time = s.start_time
 AND p.stop_sequence = s.stop_sequence;
"""

_PREDICTION_COLUMNS = (
    "trip_id", "start_date", "start_time", "stop_sequence", "stop_id",
    "scheduled_arrival", "scheduled_departure", "source_row",
    "predicted_arrival", "predicted_departure",
    "arrival_basis", "departure_basis",
    "arrival_src_sequence", "departure_src_sequence",
    "arrival_src_event", "departure_src_event",
    "null_reason", "stop_status", "entity_id",
)


class LiveArrivals:
    """SQLite-backed live arrivals warehouse bound to one fixed catalog."""

    def __init__(self, database, timetable):
        self.database = str(database)
        # Accept either a TimetableService or a plain catalog directory path;
        # only the on-disk catalog is ever read.
        catalog = getattr(timetable, "catalog", timetable)
        try:
            self.reader = CatalogReader(catalog)
        except TimetableError as exc:
            raise LiveFeedError(f"cannot bind catalog: {exc}") from exc
        self.catalog_path = str(Path(catalog).resolve())
        self.catalog_id = self.reader.catalog_id

        self.db = sqlite3.connect(self.database)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript(_SCHEMA_SQL)
        self._bind_catalog()
        self._reconcile()

    # ------------------------------------------------------------- catalog bind
    def _bind_catalog(self):
        row = self.db.execute(
            "SELECT catalog_path, catalog_id FROM catalog_binding WHERE id = 1"
        ).fetchone()
        if row is None:
            with self.db:
                self.db.execute(
                    "INSERT INTO catalog_binding(id, catalog_path, catalog_id) "
                    "VALUES (1, ?, ?)",
                    (self.catalog_path, self.catalog_id),
                )
                self.db.executemany(
                    "INSERT INTO static_calls VALUES (?,?,?,?,?,?,?,?)",
                    [
                        (
                            call["trip_id"], call["start_date"], call["start_time"],
                            call["stop_sequence"], call["stop_id"],
                            call["arrival"], call["departure"], call["source_row"],
                        )
                        for call in self.reader.rows
                    ],
                )
            return
        if row["catalog_id"] != self.catalog_id:
            raise LiveFeedError(
                f"database {self.database!r} is bound to catalog "
                f"{row['catalog_id'][:12]}... but {self.catalog_path} has id "
                f"{self.catalog_id[:12]}...; a warehouse keeps one fixed catalog"
            )

    def _project_entity(self, entity):
        """Validate one stored/feed entity against the catalog and project it."""
        label = f"entity {entity.id!r}"
        update = entity.trip_update
        trip = update.trip
        key = (trip.trip_id, trip.start_date, trip.start_time)
        try:
            calls = self.reader.lookup(*key)
        except KeyError as exc:
            raise LiveFeedError(f"{label}: {exc}") from exc
        relationship = (
            ALLOWED_TRIP_RELATIONSHIPS[trip.schedule_relationship]
            if trip.HasField("schedule_relationship") else "SCHEDULED"
        )
        if relationship == "CANCELED":
            if update.stop_time_update:
                raise LiveFeedError(
                    f"{label}: CANCELED trip must not contain stop_time_update"
                )
            rows = _project_canceled(calls)
        else:
            rows = _project_scheduled(calls, update, label)
        for row in rows:
            row["entity_id"] = entity.id
        return key, relationship, rows

    def _rebuild_predictions(self, entities):
        """Project every active entity; one entity per instance is enforced."""
        owners = {}
        all_rows = []
        for entity in entities:
            key, _relationship, rows = self._project_entity(entity)
            if key in owners:
                raise LiveFeedError(
                    f"instance {key} claimed by multiple entities: "
                    f"{owners[key]!r} and {entity.id!r}"
                )
            owners[key] = entity.id
            all_rows.extend(rows)
        return all_rows

    def _load_active_entities(self):
        entities = []
        for row in self.db.execute(
            "SELECT payload FROM active_entities ORDER BY entity_id"
        ):
            entity = gtfs.FeedEntity()
            try:
                entity.ParseFromString(row["payload"])
            except Exception as exc:  # corrupted durable state
                raise LiveFeedError(f"stored entity cannot be decoded: {exc}") from exc
            entities.append(entity)
        return entities

    def _reconcile(self):
        """Reopen check: durable entities must re-explain the stored view."""
        entities = self._load_active_entities()
        expected = self._rebuild_predictions(entities)
        stored = self.db.execute(
            f"SELECT {', '.join(_PREDICTION_COLUMNS)} FROM predictions "
            "ORDER BY trip_id, start_date, start_time, stop_sequence"
        ).fetchall()
        expected_tuples = [
            tuple(row[column] for column in _PREDICTION_COLUMNS)
            for row in sorted(
                expected,
                key=lambda r: (r["trip_id"], r["start_date"], r["start_time"],
                               r["stop_sequence"]),
            )
        ]
        stored_tuples = [tuple(row) for row in stored]
        if stored_tuples != expected_tuples:
            raise LiveFeedError(
                "stored predictions do not match active entities; "
                "database is inconsistent"
            )
        static_stored = self.db.execute(
            "SELECT COUNT(*) AS n FROM static_calls"
        ).fetchone()["n"]
        if static_stored != len(self.reader.rows):
            raise LiveFeedError(
                "stored static_calls snapshot differs from the bound catalog"
            )

    # -------------------------------------------------------------------- apply
    def apply(self, feed_bytes):
        """Validate and commit one feed; returns a small acceptance summary."""
        if not isinstance(feed_bytes, (bytes, bytearray)):
            raise LiveFeedError("feed payload must be protobuf bytes")
        feed = gtfs.FeedMessage()
        try:
            feed.ParseFromString(bytes(feed_bytes))
        except Exception as exc:
            raise LiveFeedError(f"feed is not a decodable FeedMessage: {exc}") from exc

        incrementality = _validate_feed(
            feed,
            allow_deletes=(
                feed.header.HasField("incrementality")
                and feed.header.incrementality == gtfs.FeedHeader.DIFFERENTIAL
            ),
        )
        timestamp = feed.header.timestamp

        prior = self.db.execute(
            "SELECT last_timestamp, last_payload FROM feed_state WHERE id = 1"
        ).fetchone()
        if prior is not None:
            if timestamp < prior["last_timestamp"]:
                raise LiveFeedError(
                    f"feed timestamp {timestamp} is older than accepted "
                    f"{prior['last_timestamp']}"
                )
            if timestamp == prior["last_timestamp"]:
                if bytes(feed_bytes) == bytes(prior["last_payload"]):
                    return {
                        "timestamp": timestamp,
                        "incrementality": incrementality,
                        "accepted": False,
                        "idempotent_replay": True,
                        "entity_count": len(feed.entity),
                    }
                raise LiveFeedError(
                    f"timestamp {timestamp} already accepted with different bytes"
                )

        # Compose the candidate active set and fully project it *before* any
        # write, so validation failures cannot leave a partial view behind.
        if incrementality == "FULL_DATASET":
            candidate = {}
        else:
            candidate = {
                entity.id: entity
                for entity in self._load_active_entities()
            }

        for entity in feed.entity:
            if entity.is_deleted:
                candidate.pop(entity.id, None)  # deleting an unknown id is a no-op
                continue
            candidate[entity.id] = entity

        # Projection also performs catalog lookup, selector and conflict checks.
        projected_rows = self._rebuild_predictions(list(candidate.values()))

        payload_sha = hashlib.sha256(bytes(feed_bytes)).hexdigest()
        applied_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            with self.db:  # one transaction: any error rolls the whole feed back
                self.db.execute("DELETE FROM active_entities")
                self.db.executemany(
                    "INSERT INTO active_entities VALUES (?,?,?,?,?,?)",
                    [
                        (
                            entity.id,
                            entity.SerializeToString(),
                            entity.trip_update.trip.trip_id,
                            entity.trip_update.trip.start_date,
                            entity.trip_update.trip.start_time,
                            ALLOWED_TRIP_RELATIONSHIPS[
                                entity.trip_update.trip.schedule_relationship
                            ] if entity.trip_update.trip.HasField("schedule_relationship")
                            else "SCHEDULED",
                        )
                        for entity in candidate.values()
                    ],
                )
                self.db.execute("DELETE FROM predictions")
                self.db.executemany(
                    f"INSERT INTO predictions VALUES "
                    f"({', '.join('?' for _ in _PREDICTION_COLUMNS)})",
                    [
                        tuple(row[column] for column in _PREDICTION_COLUMNS)
                        for row in projected_rows
                    ],
                )
                self.db.execute(
                    "INSERT OR REPLACE INTO feed_state(id, last_timestamp, "
                    "last_payload, incrementality) VALUES (1, ?, ?, ?)",
                    (timestamp, bytes(feed_bytes), incrementality),
                )
                self.db.execute(
                    "INSERT INTO feed_log VALUES (?,?,?,?,?,?)",
                    (
                        timestamp, bytes(feed_bytes), incrementality,
                        len(feed.entity), payload_sha, applied_at,
                    ),
                )
        except sqlite3.Error as exc:
            raise LiveFeedError(f"sqlite commit failed and rolled back: {exc}") from exc

        return {
            "timestamp": timestamp,
            "incrementality": incrementality,
            "accepted": True,
            "idempotent_replay": False,
            "entity_count": len(feed.entity),
            "active_entities": len(candidate),
            "prediction_rows": len(projected_rows),
            "payload_sha256": payload_sha,
        }

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------- query
    def query(self, stop_id=None, lo=None, hi=None, instance=None):
        """Return the joined static+live view, always keeping static rows.

        Filters combine an optional stop_id with a scheduled-departure window
        [lo, hi) and an optional exact instance key
        ``(trip_id, start_date, start_time)``. Repeated visits to one stop are
        returned as separate rows.
        """
        clauses, params = [], []
        if stop_id is not None:
            clauses.append("stop_id = ?")
            params.append(stop_id)
        if lo is not None:
            clauses.append("scheduled_departure >= ?")
            params.append(lo)
        if hi is not None:
            clauses.append("scheduled_departure < ?")
            params.append(hi)
        if instance is not None:
            trip_id, start_date, start_time = instance
            clauses.extend([
                "trip_id = ?", "start_date = ?", "start_time = ?",
            ])
            params.extend([trip_id, start_date, start_time])
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT * FROM arrivals" + where +
            " ORDER BY trip_id, start_date, start_time, stop_sequence"
        )
        return [dict(row) for row in self.db.execute(sql, params)]

    def accepted_feeds(self):
        return [
            dict(row) for row in self.db.execute(
                "SELECT feed_timestamp, incrementality, entity_count, "
                "payload_sha256, applied_at FROM feed_log "
                "ORDER BY feed_timestamp, payload_sha256"
            )
        ]

    def inspect(self):
        """Catalog identity, accepted feeds, active entities and every source."""
        binding = self.db.execute(
            "SELECT * FROM catalog_binding WHERE id = 1"
        ).fetchone()
        state = self.db.execute("SELECT * FROM feed_state WHERE id = 1").fetchone()
        active = [
            dict(row) for row in self.db.execute(
                "SELECT entity_id, trip_id, start_date, start_time, "
                "schedule_relationship FROM active_entities ORDER BY entity_id"
            )
        ]
        counts = {
            row["stop_status"]: row["n"]
            for row in self.db.execute(
                "SELECT stop_status, COUNT(*) AS n FROM arrivals "
                "GROUP BY stop_status"
            )
        }
        return {
            "catalog": {
                "path": binding["catalog_path"],
                "catalog_id": binding["catalog_id"],
            },
            "last_feed": (
                None if state is None else {
                    "timestamp": state["last_timestamp"],
                    "incrementality": state["incrementality"],
                    "payload_sha256": hashlib.sha256(
                        bytes(state["last_payload"])
                    ).hexdigest(),
                }
            ),
            "accepted_feeds": self.accepted_feeds(),
            "active_entities": active,
            "prediction_counts_by_status": counts,
            "arrivals": self.query(),
        }
