"""Current full-feed loader: ordinary positive-delay feeds are supported."""
import json
import sqlite3
from google.transit import gtfs_realtime_pb2 as gtfs


class LiveArrivals:
    def __init__(self, database, timetable):
        self.database = str(database)
        self.timetable = timetable
        with sqlite3.connect(self.database) as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS feed_state(id INTEGER PRIMARY KEY, payload BLOB);
              CREATE TABLE IF NOT EXISTS predictions(
                trip_id TEXT, start_date TEXT, start_time TEXT, stop_sequence INTEGER,
                stop_id TEXT, scheduled_arrival INTEGER, scheduled_departure INTEGER,
                predicted_arrival INTEGER, predicted_departure INTEGER, status TEXT,
                entity_id TEXT);
            """)

    def apply(self, feed_bytes):
        feed = gtfs.FeedMessage.FromString(feed_bytes)
        rows = []
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            update = entity.trip_update
            trip = update.trip
            calls = self.timetable.lookup(trip.trip_id, trip.start_date, trip.start_time)
            updates = {u.stop_id: u for u in update.stop_time_update}
            delay = 0
            for call in calls:
                item = updates.get(call["stop_id"])
                if item is not None and item.HasField("arrival"):
                    event = item.arrival
                    if event.HasField("delay"):
                        delay = event.delay
                    elif event.HasField("time"):
                        delay = event.time - call["arrival"]
                rows.append((
                    trip.trip_id, trip.start_date, trip.start_time,
                    call["stop_sequence"], call["stop_id"], call["arrival"], call["departure"],
                    call["arrival"] + delay, call["departure"] + delay,
                    "predicted", entity.id,
                ))
        with sqlite3.connect(self.database) as db:
            db.execute("DELETE FROM predictions")
            db.executemany("INSERT INTO predictions VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
            db.execute("INSERT OR REPLACE INTO feed_state VALUES(1,?)", (feed_bytes,))
        return len(rows)

    def query(self):
        with sqlite3.connect(self.database) as db:
            db.row_factory = sqlite3.Row
            return [dict(r) for r in db.execute(
                "SELECT * FROM predictions ORDER BY trip_id,start_date,start_time,stop_sequence"
            )]

    def inspect(self):
        return {"predictions": self.query()}
