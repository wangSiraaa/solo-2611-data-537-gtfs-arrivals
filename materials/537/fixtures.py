"""Synthetic CSV and actual protobuf fixtures; no network service is needed."""
import csv
from pathlib import Path
from google.transit import gtfs_realtime_pb2 as gtfs


def write_schedule(directory, calls, instances=None):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    tables = {
        "trips.txt": (["trip_id"], [{"trip_id": "T"}]),
        "stop_times.txt": (
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
            [dict(trip_id="T", arrival_time=t, departure_time=t, stop_id=s, stop_sequence=n)
             for n, s, t in calls],
        ),
        "instances.txt": (
            ["trip_id", "start_date", "start_time"],
            instances or [{"trip_id": "T", "start_date": "20260101", "start_time": calls[0][2]}],
        ),
    }
    for name, (columns, rows) in tables.items():
        with (directory / name).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    return directory


def make_feed(updates, *, start_time="08:00:00", timestamp=1):
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs.FeedHeader.FULL_DATASET
    feed.header.timestamp = timestamp
    entity = feed.entity.add()
    entity.id = "vehicle-T"
    trip = entity.trip_update.trip
    trip.trip_id = "T"
    trip.start_date = "20260101"
    trip.start_time = start_time
    for fields in updates:
        item = entity.trip_update.stop_time_update.add()
        if "stop_id" in fields:
            item.stop_id = fields["stop_id"]
        if "stop_sequence" in fields:
            item.stop_sequence = fields["stop_sequence"]
        if "relationship" in fields:
            item.schedule_relationship = fields["relationship"]
        for field in ("arrival", "departure"):
            if field in fields:
                for key, value in fields[field].items():
                    setattr(getattr(item, field), key, value)
    return feed.SerializeToString()
