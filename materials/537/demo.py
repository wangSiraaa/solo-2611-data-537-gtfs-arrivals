import json
from pathlib import Path
from timetable import TimetableService
from arrivals import LiveArrivals
from fixtures import write_schedule, make_feed


if __name__ == "__main__":
    root = Path("demo-output")
    root.mkdir(exist_ok=True)
    source = write_schedule(root / "source", [(10, "A", "08:00:00"), (20, "B", "08:10:00"), (30, "C", "08:20:00")])
    timetable = TimetableService(root / "catalog.json")
    timetable.publish(source)
    live = LiveArrivals(root / "predictions.sqlite", timetable)
    feed = make_feed([{"stop_id": "A", "stop_sequence": 10, "arrival": {"delay": 120}}])
    (root / "feed.pb").write_bytes(feed)
    live.apply(feed)
    print(json.dumps(live.inspect(), indent=2))
