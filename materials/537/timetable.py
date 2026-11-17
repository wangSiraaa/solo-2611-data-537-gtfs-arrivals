"""Static timetable instance service.

Reads GTFS-shaped ``trips.txt`` / ``stop_times.txt`` plus an explicit
``instances.txt`` listing ``(trip_id, start_date, start_time)`` and publishes an
independent, on-disk ``scheduled-calls/v1`` catalog that any consumer (including
the live arrivals warehouse) can read without touching this publisher's memory.

Times are GTFS wall-clock times where HH may range 0..47: values are never taken
modulo 24, so a 24:10:00 departure stays on the *same* service day at 87000
seconds. All published instants are UTC integer seconds (the service timezone
is fixed at UTC+08).
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path

ZONE = dt.timezone(dt.timedelta(hours=8))
SCHEMA = "scheduled-calls/v1"
MAX_SEQUENCE = 2**32 - 1
_DIGITS = re.compile(r"^\d+$")


class TimetableError(ValueError):
    """Raised when source data cannot produce a valid catalog."""


def parse_hms(value, *, hh_limit=47):
    """Parse ``HH:MM:SS`` (HH 0..hh_limit, MM/SS 0..59) into raw seconds.

    No modulo is applied. ``"24:10:00"`` is 87000 seconds, not 600.
    """
    if not isinstance(value, str):
        raise TimetableError(f"time must be a string, got {value!r}")
    parts = value.split(":")
    if len(parts) != 3 or any(not _DIGITS.fullmatch(p) for p in parts):
        raise TimetableError(f"illegal time {value!r}, want HH:MM:SS")
    hh, mm, ss = (int(p) for p in parts)
    if hh > hh_limit or mm > 59 or ss > 59:
        raise TimetableError(f"time component out of range in {value!r}")
    return hh * 3600 + mm * 60 + ss


def parse_service_day(value):
    """Validate a ``YYYYMMDD`` service day and return its UTC midnight epoch."""
    if not isinstance(value, str) or not re.fullmatch(r"\d{8}", value):
        raise TimetableError(f"illegal start_date {value!r}, want YYYYMMDD")
    try:
        day = dt.datetime.strptime(value, "%Y%m%d").replace(tzinfo=ZONE)
    except ValueError as exc:
        raise TimetableError(f"illegal calendar date {value!r}: {exc}") from exc
    return int(day.timestamp())


def parse_sequence(value):
    """Validate a stop_sequence in 0..2**32-1 (integers, no sign/decimals)."""
    if not isinstance(value, str) or not _DIGITS.fullmatch(value):
        raise TimetableError(f"illegal stop_sequence {value!r}")
    sequence = int(value)
    if sequence > MAX_SEQUENCE:
        raise TimetableError(f"stop_sequence {sequence} exceeds 2**32-1")
    return sequence


def _require_columns(fieldnames, required, filename):
    missing = [c for c in required if c not in fieldnames]
    if missing:
        raise TimetableError(f"{filename} missing columns: {', '.join(missing)}")


def _read_dicts(path):
    if not path.exists():
        raise TimetableError(f"required file not found: {path.name}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise TimetableError(f"{path.name} has no header row")
        return reader.fieldnames, list(reader)


class CatalogReader:
    """Read-only client for a published ``scheduled-calls/v1`` directory.

    This is the only coupling between publishers and consumers: it understands
    the documented on-disk row format, never the publisher's private objects.
    A catalog produced by another publisher works as long as it follows the
    same manifest + ``calls.csv`` shape and ordering constraints.
    """

    def __init__(self, catalog):
        self.catalog = Path(catalog)
        manifest_path = self.catalog / "manifest.json"
        calls_path = self.catalog / "calls.csv"
        if not manifest_path.exists() or not calls_path.exists():
            raise TimetableError(f"not a {SCHEMA} catalog directory: {self.catalog}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema") != SCHEMA:
            raise TimetableError(
                f"unsupported catalog schema {manifest.get('schema')!r}, want {SCHEMA}"
            )
        raw = calls_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if digest != manifest.get("sha256"):
            raise TimetableError(f"catalog checksum mismatch in {self.catalog}")
        self.manifest = manifest
        self.catalog_id = digest
        self.rows = []
        with calls_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                self.rows.append({
                    "trip_id": row["trip_id"],
                    "start_date": row["start_date"],
                    "start_time": row["start_time"],
                    "stop_sequence": int(row["stop_sequence"]),
                    "stop_id": row["stop_id"],
                    "arrival": int(row["arrival"]),
                    "departure": int(row["departure"]),
                    "source_row": int(row["source_row"]),
                })
        self._by_key = {}
        for row in self.rows:
            key = (row["trip_id"], row["start_date"], row["start_time"])
            self._by_key.setdefault(key, []).append(row)
        for calls in self._by_key.values():
            sequences = [c["stop_sequence"] for c in calls]
            if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
                raise TimetableError("catalog calls.csv violates ordering constraints")

    def lookup(self, trip_id, start_date, start_time):
        key = (trip_id, start_date, start_time)
        if key not in self._by_key:
            raise KeyError(f"unknown instance {key}")
        return [dict(call) for call in self._by_key[key]]

    def query(self, stop_id, lo, hi):
        return [
            dict(row)
            for row in self.rows
            if row["stop_id"] == stop_id and lo <= row["departure"] < hi
        ]


class TimetableService:
    """Publish and query scheduled call catalogs (no live warehouse needed)."""

    def __init__(self, catalog):
        self.catalog = Path(catalog)

    # ------------------------------------------------------------------ build
    def _build(self, source):
        source = Path(source)
        trip_fields, trip_rows = _read_dicts(source / "trips.txt")
        _require_columns(trip_fields, ["trip_id"], "trips.txt")
        trips = set()
        for row in trip_rows:
            trip_id = row["trip_id"]
            if not trip_id:
                raise TimetableError("trips.txt contains an empty trip_id")
            if trip_id in trips:
                raise TimetableError(f"duplicate trip_id in trips.txt: {trip_id}")
            trips.add(trip_id)

        st_fields, st_rows = _read_dicts(source / "stop_times.txt")
        _require_columns(
            st_fields,
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
            "stop_times.txt",
        )
        templates = {}
        for source_row, row in enumerate(st_rows, start=2):
            trip_id = row["trip_id"]
            if trip_id not in trips:
                raise TimetableError(
                    f"stop_times.txt line {source_row}: unknown trip_id {trip_id!r}"
                )
            if not row["stop_id"]:
                raise TimetableError(
                    f"stop_times.txt line {source_row}: empty stop_id"
                )
            sequence = parse_sequence(row["stop_sequence"])
            arrival = parse_hms(row["arrival_time"])
            departure = parse_hms(row["departure_time"])
            if arrival > departure:
                raise TimetableError(
                    f"stop_times.txt line {source_row}: arrival after departure"
                )
            call = {
                "stop_sequence": sequence,
                "stop_id": row["stop_id"],
                "arrival_seconds": arrival,
                "departure_seconds": departure,
                "source_row": source_row,
            }
            templates.setdefault(trip_id, []).append(call)

        for trip_id, calls in templates.items():
            calls.sort(key=lambda c: c["stop_sequence"])
            seen = set()
            previous_departure = None
            for call in calls:
                if call["stop_sequence"] in seen:
                    raise TimetableError(
                        f"trip {trip_id!r}: duplicate stop_sequence "
                        f"{call['stop_sequence']}"
                    )
                seen.add(call["stop_sequence"])
                if (
                    previous_departure is not None
                    and call["arrival_seconds"] < previous_departure
                ):
                    raise TimetableError(
                        f"trip {trip_id!r}: stop_sequence {call['stop_sequence']} "
                        f"arrives before the previous stop departs"
                    )
                previous_departure = call["departure_seconds"]

        inst_fields, inst_rows = _read_dicts(source / "instances.txt")
        _require_columns(
            inst_fields, ["trip_id", "start_date", "start_time"], "instances.txt"
        )
        published = []
        seen_keys = set()
        for row in inst_rows:
            trip_id = row["trip_id"]
            if trip_id not in trips:
                raise TimetableError(f"instances.txt: unknown trip_id {trip_id!r}")
            start_date = row["start_date"]
            start_time = row["start_time"]
            day_ts = parse_service_day(start_date)
            start_seconds = parse_hms(start_time)
            key = (trip_id, start_date, start_time)
            if key in seen_keys:
                raise TimetableError(f"duplicate instance key: {key}")
            seen_keys.add(key)
            calls = templates.get(trip_id)
            if not calls:
                raise TimetableError(
                    f"instance {key}: trip {trip_id!r} has no stop_times rows"
                )
            shift = start_seconds - calls[0]["departure_seconds"]
            for call in calls:
                published.append({
                    "trip_id": trip_id,
                    "start_date": start_date,
                    "start_time": start_time,
                    "stop_sequence": call["stop_sequence"],
                    "stop_id": call["stop_id"],
                    "arrival": day_ts + call["arrival_seconds"] + shift,
                    "departure": day_ts + call["departure_seconds"] + shift,
                    "source_row": call["source_row"],
                })

        published.sort(
            key=lambda r: (r["trip_id"], r["start_date"], r["start_time"],
                           r["stop_sequence"])
        )
        meta = {
            "trip_count": len(trips),
            "instance_count": len(seen_keys),
            "call_count": len(published),
        }
        return published, meta

    # ---------------------------------------------------------------- publish
    def publish(self, source):
        """Validate everything, then (and only then) replace the catalog.

        A failed validation raises :class:`TimetableError` and leaves any
        previously published directory byte-for-byte untouched.
        """
        rows, meta = self._build(source)
        catalog = self.catalog
        catalog.mkdir(parents=True, exist_ok=True)

        def write_atomic(name, payload):
            tmp = catalog / f".{name}.tmp"
            tmp.write_bytes(payload)
            os.replace(tmp, catalog / name)

        header = [
            "trip_id", "start_date", "start_time", "stop_sequence", "stop_id",
            "arrival", "departure", "source_row",
        ]
        import io
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
        csv_bytes = buffer.getvalue().encode("utf-8")
        digest = hashlib.sha256(csv_bytes).hexdigest()
        manifest = {
            "schema": SCHEMA,
            "published_at": dt.datetime.now(dt.timezone.utc)
            .isoformat(timespec="seconds"),
            "source": str(Path(source)),
            **meta,
            "sha256": digest,
        }
        # calls.csv first, then the manifest that seals its checksum.
        write_atomic("calls.csv", csv_bytes)
        write_atomic("manifest.json",
                     (json.dumps(manifest, indent=2, sort_keys=True) + "\n")
                     .encode("utf-8"))
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ query
    def _reader(self):
        return CatalogReader(self.catalog)

    def lookup(self, trip_id, start_date, start_time):
        """Exact read of one complete instance key, ordered by stop_sequence."""
        return self._reader().lookup(trip_id, start_date, start_time)

    def query(self, stop_id, lo, hi):
        """Scheduled departures in [lo, hi) for one stop_id.

        Repeated visits to the same stop_id produce one row each.
        """
        return self._reader().query(stop_id, lo, hi)
