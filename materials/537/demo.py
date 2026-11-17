"""End-to-end demo: three-stop trip with a +120s arrival delay at the first stop.

Run from this directory with the project venv active::

    python demo.py

The delay is established by an explicit arrival event at stop A and must
propagate to every later arrival/departure, each row recording the source stop
it was propagated from.
"""
import json
from pathlib import Path

from timetable import TimetableService
from arrivals import LiveArrivals
from fixtures import write_schedule, make_feed


def main():
    root = Path("demo-output")
    root.mkdir(exist_ok=True)
    source = write_schedule(
        root / "source",
        [(10, "A", "08:00:00"), (20, "B", "08:10:00"), (30, "C", "08:20:00")],
    )
    catalog = root / "catalog"
    timetable = TimetableService(catalog)
    rows = timetable.publish(source)

    live = LiveArrivals(root / "predictions.sqlite", timetable)
    feed = make_feed([{"stop_id": "A", "stop_sequence": 10,
                       "arrival": {"delay": 120}}])
    (root / "feed.pb").write_bytes(feed)
    summary = live.apply(feed)

    print("== published catalog:", catalog, "==")
    for row in rows:
        print(json.dumps(row, ensure_ascii=False))
    print("\n== feed acceptance ==")
    print(json.dumps(summary, indent=2))
    print("\n== arrivals view ==")
    for row in live.query():
        print(json.dumps(row, ensure_ascii=False))
    live.close()


if __name__ == "__main__":
    main()
