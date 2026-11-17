"""Command line entry points for the timetable service and live warehouse.

All commands work on the on-disk catalog / SQLite database, so they also
demonstrate that a live warehouse consumes only another publisher's directory::

  python cli.py publish   --source DIR --catalog DIR
  python cli.py lookup    --catalog DIR --trip T --date 20260101 --time 08:00:00
  python cli.py squery    --catalog DIR --stop A --lo EPOCH --hi EPOCH
  python cli.py apply     --db FILE --catalog DIR --feed FILE.pb
  python cli.py replay    --db FILE --catalog DIR --feed-dir DIR
  python cli.py query     --db FILE [--stop A] [--lo EPOCH] [--hi EPOCH]
                                      [--trip T --date ... --time ...]
  python cli.py inspect   --db FILE
"""
import argparse
import json
import sys
from pathlib import Path

from timetable import TimetableService, TimetableError
from arrivals import LiveArrivals, LiveFeedError


def _print(value):
    print(json.dumps(value, indent=2, ensure_ascii=False))


def cmd_publish(args):
    service = TimetableService(args.catalog)
    rows = service.publish(args.source)
    _print({"catalog": str(args.catalog), "published_calls": len(rows)})


def cmd_lookup(args):
    _print(TimetableService(args.catalog).lookup(
        args.trip, args.date, args.time))


def cmd_squery(args):
    _print(TimetableService(args.catalog).query(args.stop, args.lo, args.hi))


def _open_warehouse(args):
    return LiveArrivals(args.db, args.catalog)


def cmd_apply(args):
    payload = Path(args.feed).read_bytes()
    with _open_warehouse(args) as live:
        _print(live.apply(payload))


def cmd_replay(args):
    feed_dir = Path(args.feed_dir)
    results = []
    with _open_warehouse(args) as live:
        for path in sorted(feed_dir.glob("*.pb")):
            payload = path.read_bytes()
            try:
                outcome = live.apply(payload)
            except LiveFeedError as exc:
                outcome = {"file": path.name, "rejected": str(exc)}
            else:
                outcome = {"file": path.name, **outcome}
            results.append(outcome)
    _print(results)


def cmd_query(args):
    instance = None
    if args.trip is not None:
        if args.date is None or args.time is None:
            raise SystemExit("--trip requires --date and --time")
        instance = (args.trip, args.date, args.time)
    with _open_warehouse(args) as live:
        _print(live.query(
            stop_id=args.stop, lo=args.lo, hi=args.hi, instance=instance))


def cmd_inspect(args):
    with _open_warehouse(args) as live:
        _print(live.inspect())


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("publish")
    p.add_argument("--source", required=True)
    p.add_argument("--catalog", required=True)
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("lookup")
    p.add_argument("--catalog", required=True)
    p.add_argument("--trip", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--time", required=True)
    p.set_defaults(func=cmd_lookup)

    p = sub.add_parser("squery")
    p.add_argument("--catalog", required=True)
    p.add_argument("--stop", required=True)
    p.add_argument("--lo", required=True, type=int)
    p.add_argument("--hi", required=True, type=int)
    p.set_defaults(func=cmd_squery)

    p = sub.add_parser("apply")
    p.add_argument("--db", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--feed", required=True)
    p.set_defaults(func=cmd_apply)

    p = sub.add_parser("replay")
    p.add_argument("--db", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--feed-dir", required=True)
    p.set_defaults(func=cmd_replay)

    p = sub.add_parser("query")
    p.add_argument("--db", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--stop")
    p.add_argument("--lo", type=int)
    p.add_argument("--hi", type=int)
    p.add_argument("--trip")
    p.add_argument("--date")
    p.add_argument("--time")
    p.set_defaults(func=cmd_query)

    p = sub.add_parser("inspect")
    p.add_argument("--db", required=True)
    p.add_argument("--catalog", required=True)
    p.set_defaults(func=cmd_inspect)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except (TimetableError, LiveFeedError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
