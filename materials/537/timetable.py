"""Small timetable publisher used independently of the live-arrival service."""
import csv
import datetime as dt
import json
from pathlib import Path

ZONE = dt.timezone(dt.timedelta(hours=8))


def time_seconds(value):
    hh, mm, ss = map(int, value.split(":"))
    return (hh % 24) * 3600 + mm * 60 + ss


class TimetableService:
    def __init__(self, catalog):
        self.catalog = Path(catalog)

    def publish(self, source):
        source = Path(source)
        with (source / "trips.txt").open(newline="", encoding="utf-8") as f:
            trips = {r["trip_id"] for r in csv.DictReader(f)}
        calls = {}
        with (source / "stop_times.txt").open(newline="", encoding="utf-8") as f:
            for source_row, row in enumerate(csv.DictReader(f), 2):
                if row["trip_id"] not in trips:
                    raise ValueError("unknown trip")
                call = dict(row, source_row=source_row)
                calls.setdefault(row["trip_id"], {})[row["stop_id"]] = call
        result = []
        with (source / "instances.txt").open(newline="", encoding="utf-8") as f:
            for instance in csv.DictReader(f):
                day = dt.datetime.strptime(instance["start_date"], "%Y%m%d").replace(tzinfo=ZONE)
                rows = sorted(calls[instance["trip_id"]].values(), key=lambda x: int(x["stop_sequence"]))
                base = time_seconds(rows[0]["departure_time"])
                shift = time_seconds(instance["start_time"]) - base
                for row in rows:
                    result.append({
                        **instance,
                        "stop_sequence": int(row["stop_sequence"]),
                        "stop_id": row["stop_id"],
                        "arrival": int(day.timestamp()) + time_seconds(row["arrival_time"]) + shift,
                        "departure": int(day.timestamp()) + time_seconds(row["departure_time"]) + shift,
                        "source_row": row["source_row"],
                    })
        self.catalog.parent.mkdir(parents=True, exist_ok=True)
        self.catalog.write_text(json.dumps({"schema": "scheduled-calls/v1", "calls": result}), encoding="utf-8")
        return result

    def lookup(self, trip_id, start_date, start_time):
        rows = json.loads(self.catalog.read_text(encoding="utf-8"))["calls"]
        return sorted((r for r in rows if r["trip_id"] == trip_id), key=lambda x: x["stop_sequence"])

    def query(self, stop_id, lo, hi):
        rows = json.loads(self.catalog.read_text(encoding="utf-8"))["calls"]
        return [r for r in rows if r["stop_id"] == stop_id and lo <= r["departure"] < hi]
