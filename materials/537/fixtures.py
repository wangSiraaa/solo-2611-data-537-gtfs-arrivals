"""Synthetic CSV and real protobuf fixtures; no network service is needed.

Feeds are built with the standard ``gtfs-realtime-bindings`` generated message
classes and serialized to genuine protobuf bytes (the test suite also writes
them through ``.pb`` files and re-reads them from disk).
"""
import csv
from pathlib import Path

from google.transit import gtfs_realtime_pb2 as gtfs


def write_tables(directory, trips, stop_times, instances):
    """Write the three source CSVs with explicit column sets."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    tables = {
        "trips.txt": (["trip_id"], [{"trip_id": t} for t in trips]),
        "stop_times.txt": (
            ["trip_id", "arrival_time", "departure_time", "stop_id",
             "stop_sequence"],
            stop_times,
        ),
        "instances.txt": (
            ["trip_id", "start_date", "start_time"], instances,
        ),
    }
    for name, (columns, rows) in tables.items():
        with (directory / name).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    return directory


def write_schedule(directory, calls, instances=None, trip_id="T"):
    """Original one-trip helper: ``calls`` is (stop_sequence, stop_id, time)."""
    stop_times = [
        dict(trip_id=trip_id, arrival_time=time, departure_time=time,
             stop_id=stop, stop_sequence=sequence)
        for sequence, stop, time in calls
    ]
    if instances is None:
        instances = [{
            "trip_id": trip_id,
            "start_date": "20260101",
            "start_time": calls[0][2],
        }]
    return write_tables(directory, [trip_id], stop_times, instances)


def _fill_event(event, fields):
    # Only time/delay are ever set; both are plain integer seconds.
    if "time" in fields:
        event.time = fields["time"]
    if "delay" in fields:
        event.delay = fields["delay"]


def _add_entity(feed, spec):
    entity = feed.entity.add()
    entity.id = spec["id"]
    if spec.get("is_deleted"):
        entity.is_deleted = True
        return entity
    trip = entity.trip_update.trip
    key = spec.get("key", ("T", "20260101", "08:00:00"))
    trip.trip_id, trip.start_date, trip.start_time = key
    if "trip_relationship" in spec:
        trip.schedule_relationship = {
            "SCHEDULED": gtfs.TripDescriptor.SCHEDULED,
            "CANCELED": gtfs.TripDescriptor.CANCELED,
            "ADDED": gtfs.TripDescriptor.ADDED,
        }[spec["trip_relationship"]]
    if spec.get("trip_delay") is not None:  # unsupported field, for rejection tests
        entity.trip_update.delay = spec["trip_delay"]
    if spec.get("route_id") is not None:      # unsupported field, for rejection tests
        trip.route_id = spec["route_id"]
    for update in spec.get("stops", []):
        stu = entity.trip_update.stop_time_update.add()
        if "stop_sequence" in update:
            stu.stop_sequence = update["stop_sequence"]
        if "stop_id" in update:
            stu.stop_id = update["stop_id"]
        if "relationship" in update:
            stu.schedule_relationship = {
                "SCHEDULED": gtfs.TripUpdate.StopTimeUpdate.SCHEDULED,
                "SKIPPED": gtfs.TripUpdate.StopTimeUpdate.SKIPPED,
                "NO_DATA": gtfs.TripUpdate.StopTimeUpdate.NO_DATA,
                "UNSCHEDULED": gtfs.TripUpdate.StopTimeUpdate.UNSCHEDULED,
            }[update["relationship"]]
        if "arrival" in update:
            _fill_event(stu.arrival, update["arrival"])
        if "departure" in update:
            _fill_event(stu.departure, update["departure"])
        if update.get("uncertainty") is not None:  # unsupported, rejection tests
            stu.arrival.uncertainty = update["uncertainty"]
    return entity


def build_feed(entities, *, timestamp=1, incrementality="FULL_DATASET",
               version="2.0"):
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = version
    feed.header.incrementality = {
        "FULL_DATASET": gtfs.FeedHeader.FULL_DATASET,
        "DIFFERENTIAL": gtfs.FeedHeader.DIFFERENTIAL,
    }[incrementality]
    feed.header.timestamp = timestamp
    for spec in entities:
        _add_entity(feed, spec)
    return feed.SerializeToString()


def make_feed(updates, *, start_time="08:00:00", timestamp=1):
    """Backward-compatible single-entity FULL feed for trip T / 20260101."""
    return build_feed(
        [{"id": "vehicle-T",
          "key": ("T", "20260101", start_time),
          "stops": updates}],
        timestamp=timestamp,
    )
