"""Independent verification with hand-computed expectations.

Every expected value below is calculated from the service-day epoch and the
template's second offsets (or from the fields actually present in the protobuf
message) using plain arithmetic — the projection code under test is never used
to generate its own answers.
"""
import datetime as dt
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

from google.protobuf import unknown_fields
from google.transit import gtfs_realtime_pb2 as gtfs

from arrivals import LiveArrivals, LiveFeedError
from fixtures import write_tables, build_feed
from timetable import (
    CatalogReader, TimetableError, TimetableService, parse_hms,
)

ZONE = dt.timezone(dt.timedelta(hours=8))
PASSED = []
FAILED = []


def check(name, condition, detail=""):
    if condition:
        PASSED.append(name)
        print(f"PASS {name}")
    else:
        FAILED.append((name, detail))
        print(f"FAIL {name} :: {detail}")


def day_epoch(date_text):
    """Reference epoch math, written independently of timetable.py.

    A service day starts at 00:00 UTC+08, i.e. 16:00 UTC on the prior day.
    """
    y, m, dnum = int(date_text[:4]), int(date_text[4:6]), int(date_text[6:])
    midnight_local = dt.datetime(y, m, dnum, 0, 0, tzinfo=ZONE)
    return int(midnight_local.timestamp())


def st(trip, seq, stop, arr, dep):
    return dict(trip_id=trip, arrival_time=arr, departure_time=dep,
                stop_id=stop, stop_sequence=seq)


def inst(trip, date, time):
    return dict(trip_id=trip, start_date=date, start_time=time)


def main():
    work = Path(tempfile.mkdtemp(prefix="verify-537-"))
    try:
        run_static_tests(work)
        run_realtime_tests(work)
        run_lifecycle_tests(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        for name, detail in FAILED:
            print(f"  FAILED {name}: {detail}")
        raise SystemExit(1)


# --------------------------------------------------------------------------- #
# Static service
# --------------------------------------------------------------------------- #
def run_static_tests(work):
    # A/B/A/C loop line, non-contiguous sequences, HH up to 25, arrival!=dep.
    source = work / "static-src"
    write_tables(
        source,
        trips=["T"],
        stop_times=[
            st("T", 0, "A", "08:00:00", "08:00:30"),
            st("T", 5, "B", "08:10:00", "08:11:00"),
            st("T", 9, "A", "08:20:00", "08:20:30"),
            st("T", 20, "C", "25:00:00", "25:00:30"),
        ],
        instances=[inst("T", "20260101", "08:00:30")],
    )
    catalog_dir = work / "catalog"
    service = TimetableService(catalog_dir)
    rows = service.publish(source)
    d = day_epoch("20260101")

    expected = [
        # sequence, stop, sched arrival, sched departure, source csv line
        (0, "A", d + parse_hms("08:00:00"), d + parse_hms("08:00:30"), 2),
        (5, "B", d + parse_hms("08:10:00"), d + parse_hms("08:11:00"), 3),
        (9, "A", d + parse_hms("08:20:00"), d + parse_hms("08:20:30"), 4),
        (20, "C", d + parse_hms("25:00:00"), d + parse_hms("25:00:30"), 5),
    ]
    # start_time equals first departure, so the shift is exactly zero.
    got = [(r["stop_sequence"], r["stop_id"], r["arrival"], r["departure"],
            r["source_row"]) for r in rows]
    check("static/A-B-A-C repeated stops and 25h time", got == expected,
          f"{got} != {expected}")
    check("static/25h not modulo 24",
          rows[3]["arrival"] == d + 25 * 3600
          and rows[3]["arrival"] > d + 24 * 3600)

    # query(stop_id) returns both visits to A inside a wide window.
    visits = service.query("A", d, d + 2 * 24 * 3600)
    check("static/query keeps both A visits",
          [v["stop_sequence"] for v in visits] == [0, 9])
    # half-open [lo, hi): a departure exactly at hi is excluded while the same
    # departure at an inclusive lo is kept. The earlier A visit (08:00:30) lies
    # outside both windows around the second visit (08:20:30).
    boundary = rows[2]["departure"]
    check("static/query window is half-open",
          service.query("A", boundary + 1, boundary + 10) == []
          and [r["departure"] for r in
               service.query("A", boundary, boundary + 10)] == [boundary])

    # Exact lookup by full instance key.
    one = service.lookup("T", "20260101", "08:00:30")
    check("static/lookup full key order",
          [r["stop_sequence"] for r in one] == [0, 5, 9, 20])
    try:
        service.lookup("T", "20260101", "09:00:00")
        check("static/lookup unknown key raises", False, "no error")
    except KeyError:
        check("static/lookup unknown key raises", True)

    # Another publisher can consume the directory only through CatalogReader.
    reader = CatalogReader(catalog_dir)
    check("static/CatalogReader independent access",
          reader.query("C", d, d + 3 * 24 * 3600)[0]["arrival"]
          == d + 25 * 3600)

    # Two instances of the same trip: different service day and start_time.
    source2 = work / "static-src2"
    write_tables(
        source2,
        trips=["T"],
        stop_times=[
            st("T", 1, "A", "10:00:00", "10:00:00"),
            st("T", 2, "B", "10:05:00", "10:05:00"),
        ],
        instances=[
            inst("T", "20260101", "10:00:00"),
            inst("T", "20260102", "11:30:00"),
        ],
    )
    cat2 = work / "catalog2"
    TimetableService(cat2).publish(source2)
    d2 = day_epoch("20260102")
    second = TimetableService(cat2).lookup("T", "20260102", "11:30:00")
    # shift = 11:30:00 - 10:00:00 = +5400; B moves 10:05 -> 11:35.
    check("static/second service day and shifted start_time",
          second[0]["departure"] == d2 + parse_hms("11:30:00")
          and second[1]["arrival"] == d2 + parse_hms("11:35:00"))
    check("static/start_time past midnight keeps identity (HH>24)",
          TimetableService(work / "catalog3").publish(
              _single_instance_source(work / "src3", "24:30:00")
          )[0]["departure"] == day_epoch("20260101") + parse_hms("24:30:00"))

    run_static_rejections(work)


def _single_instance_source(directory, start_time):
    write_tables(
        directory,
        trips=["T"],
        stop_times=[st("T", 1, "A", "08:00:00", "08:00:00")],
        instances=[inst("T", "20260101", start_time)],
    )
    return directory


def run_static_rejections(work):
    base_st = [st("T", 1, "A", "08:00:00", "08:00:00"),
               st("T", 2, "B", "08:10:00", "08:10:00")]
    base_inst = [inst("T", "20260101", "08:00:00")]

    def expect_reject(name, *, trips=("T",), stop_times=None, instances=None):
        src = work / f"bad-{name}"
        write_tables(src, list(trips),
                     base_st if stop_times is None else stop_times,
                     base_inst if instances is None else instances)
        target = work / f"bad-cat-{name}"
        try:
            TimetableService(target).publish(src)
            check(name, False, "accepted invalid input")
        except TimetableError:
            check(name, True)

    expect_reject(
        "reject/unknown trip in stop_times",
        trips=("T",),
        stop_times=[st("X", 1, "A", "08:00:00", "08:00:00")],
    )
    expect_reject(
        "reject/duplicate trip_id",
        trips=("T", "T"),
    )
    expect_reject(
        "reject/duplicate instance key",
        instances=[inst("T", "20260101", "08:00:00"),
                   inst("T", "20260101", "08:00:00")],
    )
    expect_reject(
        "reject/duplicate stop_sequence",
        stop_times=[st("T", 1, "A", "08:00:00", "08:00:00"),
                    st("T", 1, "B", "08:10:00", "08:10:00")],
    )
    expect_reject(
        "reject/arrival after departure",
        stop_times=[st("T", 1, "A", "08:01:00", "08:00:00")],
    )
    expect_reject(
        "reject/non-monotonic stops",
        stop_times=[st("T", 1, "A", "08:10:00", "08:10:00"),
                    st("T", 2, "B", "08:09:00", "08:09:00")],
    )
    expect_reject(
        "reject/illegal time modulo bait",
        instances=[inst("T", "20260101", "48:00:00")],
    )
    expect_reject(
        "reject/illegal date",
        instances=[inst("T", "20261301", "08:00:00")],
    )

    # A failed publish must leave the previously good catalog byte-for-byte.
    good_src = work / "keep-src"
    write_tables(good_src, ["T"], base_st, base_inst)
    good_cat = work / "keep-cat"
    TimetableService(good_cat).publish(good_src)
    before = (good_cat / "calls.csv").read_bytes()
    bad_src = work / "keep-bad-src"
    write_tables(bad_src, ["T"],
                 [st("T", 1, "A", "09:00:00", "08:00:00")], base_inst)
    try:
        TimetableService(good_cat).publish(bad_src)
    except TimetableError:
        pass
    after = (good_cat / "calls.csv").read_bytes()
    check("reject/failed publish preserves old catalog", before == after)


# --------------------------------------------------------------------------- #
# Realtime projection semantics
# --------------------------------------------------------------------------- #
def _loop_catalog(work, name="loop"):
    source = work / f"{name}-src"
    write_tables(
        source,
        trips=["T"],
        stop_times=[
            st("T", 0, "A", "08:00:00", "08:00:00"),
            st("T", 5, "B", "08:10:00", "08:11:00"),
            st("T", 9, "A", "08:20:00", "08:20:30"),
            st("T", 20, "C", "08:30:00", "08:30:30"),
        ],
        instances=[inst("T", "20260101", "08:00:00")],
    )
    catalog = work / f"{name}-cat"
    TimetableService(catalog).publish(source)
    return catalog, day_epoch("20260101")


LOOP_KEY = ("T", "20260101", "08:00:00")


def _by_sequence(rows):
    return {r["stop_sequence"]: r for r in rows}


def run_realtime_tests(work):
    catalog, d = _loop_catalog(work)
    # Hand-computed scheduled instants: template offsets plus the instance's
    # own start shift (here start_time equals the first departure, so the shift
    # is the 08:00:00 wall offset itself).
    base = d + parse_hms("08:00:00")
    sched = {
        0: (base, base),
        5: (base + 600, base + 660),
        9: (base + 1200, base + 1230),
        20: (base + 1800, base + 1830),
    }

    def warehouse(tag):
        return LiveArrivals(work / f"rt-{tag}.sqlite", catalog)

    # 1) sequence-only selector on a duplicated stop id; stop_id mismatch.
    feed = build_feed([{
        "id": "e1",
        "key": LOOP_KEY,
        "stops": [
            {"stop_sequence": 9, "arrival": {"delay": 60}},
        ],
    }], timestamp=1)
    with warehouse("sel") as live:
        live.apply(feed)
        rows = _by_sequence(live.query())
    # delay established at seq 9 arrival; earlier stops have no trust. They are
    # covered by an active entity but received no value, so the reason is
    # no_trust rather than the no-entity no_realtime state.
    check("rt/earlier stops stay null without trust",
          rows[0]["predicted_arrival"] is None
          and rows[0]["null_reason"] == "no_trust"
          and rows[5]["predicted_arrival"] is None
          and rows[5]["null_reason"] == "no_trust")
    check("rt/sequence-only picks second A visit",
          rows[9]["predicted_arrival"] == sched[9][0] + 60
          and rows[9]["arrival_basis"] == "explicit"
          and rows[9]["arrival_src_sequence"] == 9
          and rows[9]["arrival_src_event"] == "arrival")
    # departure at seq 9 and all of seq 20 propagate +60 from that event.
    check("rt/propagation records exact source stop and event",
          rows[9]["predicted_departure"] == sched[9][1] + 60
          and rows[9]["departure_basis"] == "propagated"
          and rows[9]["departure_src_sequence"] == 9
          and rows[9]["departure_src_event"] == "arrival"
          and rows[20]["predicted_arrival"] == sched[20][0] + 60
          and rows[20]["arrival_src_sequence"] == 9)

    # stop_id-only selector on duplicated stop must be rejected.
    bad = build_feed([{"id": "e", "key": LOOP_KEY,
                       "stops": [{"stop_id": "A",
                                  "arrival": {"delay": 10}}]}])
    with warehouse("sel-bad") as live:
        try:
            live.apply(bad)
            check("rt/reject ambiguous stop_id selector", False, "accepted")
        except LiveFeedError:
            check("rt/reject ambiguous stop_id selector", True)
        # contradicting stop_id + sequence
        bad2 = build_feed([{"id": "e", "key": LOOP_KEY, "stops": [
            {"stop_sequence": 9, "stop_id": "B", "arrival": {"delay": 10}}]}],
            timestamp=2)
        try:
            live.apply(bad2)
            check("rt/reject contradictory selector", False, "accepted")
        except LiveFeedError:
            check("rt/reject contradictory selector", True)

    # 2) time and delay together: time wins and re-bases the delay.
    explicit_time = sched[5][0] - 30  # 30s early relative to B arrival
    feed = build_feed([{
        "id": "e2",
        "key": LOOP_KEY,
        "stops": [
            {"stop_sequence": 5,
             "arrival": {"time": explicit_time, "delay": 999}},
        ],
    }])
    with warehouse("time") as live:
        live.apply(feed)
        rows = _by_sequence(live.query())
    check("rt/time overrides delay and re-bases",
          rows[5]["predicted_arrival"] == explicit_time
          and rows[5]["arrival_basis"] == "explicit"
          # propagated delay is time - scheduled arrival = -30, not 999
          and rows[9]["predicted_arrival"] == sched[9][0] - 30
          and rows[9]["arrival_basis"] == "propagated"
          and rows[9]["arrival_src_sequence"] == 5)

    # 3) explicit delay 0 differs from absent: predictions exist at scheduled.
    with warehouse("zero") as live:
        live.apply(build_feed([{
            "id": "e3",
            "key": LOOP_KEY,
            "stops": [{"stop_sequence": 0, "arrival": {"delay": 0}}],
        }]))
        rows = _by_sequence(live.query())
        check("rt/explicit zero delay is a real prediction",
              rows[0]["predicted_arrival"] == sched[0][0]
              and rows[0]["arrival_basis"] == "explicit"
              and rows[5]["predicted_arrival"] == sched[5][0]
              and rows[5]["arrival_basis"] == "propagated")
        # An event with neither field is not explicit and must not fake trust.
        live.apply(build_feed([{
            "id": "e3b",
            "key": LOOP_KEY,
            "stops": [{"stop_sequence": 0},
                      {"stop_sequence": 5, "arrival": {"delay": 15}}],
        }], timestamp=2))
        rows = _by_sequence(live.query())
        check("rt/empty event does not fabricate a prediction",
              rows[0]["predicted_arrival"] is None
              and rows[0]["null_reason"] == "no_trust"
              and rows[5]["predicted_arrival"] == sched[5][0] + 15)

    # 4) negative delays allowed, no monotonic enforcement.
    with warehouse("neg") as live:
        live.apply(build_feed([{
            "id": "e4",
            "key": LOOP_KEY,
            "stops": [{"stop_sequence": 0, "arrival": {"delay": -120}}],
        }]))
        rows = _by_sequence(live.query())
    check("rt/negative delay accepted",
          rows[20]["predicted_departure"] == sched[20][1] - 120
          and rows[0]["predicted_arrival"] == sched[0][0] - 120)

    # 5) SKIPPED: null + skipped reason, delay flows through.
    with warehouse("skip") as live:
        live.apply(build_feed([{
            "id": "e5",
            "key": LOOP_KEY,
            "stops": [
                {"stop_sequence": 0, "arrival": {"delay": 90}},
                {"stop_sequence": 5, "relationship": "SKIPPED"},
                {"stop_sequence": 9, "arrival": {"delay": 200}},
            ],
        }]))
        rows = _by_sequence(live.query())
    check("rt/SKIPPED nulls both fields and marks skipped",
          rows[5]["predicted_arrival"] is None
          and rows[5]["predicted_departure"] is None
          and rows[5]["stop_status"] == "skipped"
          and rows[5]["null_reason"] == "skipped")
    check("rt/SKIPPED carries delay onward until next explicit",
          rows[9]["predicted_departure"] == sched[9][1] + 200
          and rows[20]["predicted_arrival"] == sched[20][0] + 200
          and rows[20]["arrival_src_sequence"] == 9)

    # 6) NO_DATA cuts trust; a later explicit value rebuilds it.
    with warehouse("nodata") as live:
        live.apply(build_feed([{
            "id": "e6",
            "key": LOOP_KEY,
            "stops": [
                {"stop_sequence": 0, "arrival": {"delay": 90}},
                {"stop_sequence": 5, "relationship": "NO_DATA"},
                {"stop_sequence": 20, "arrival": {"delay": -15}},
            ],
        }]))
        rows = _by_sequence(live.query())
    check("rt/NO_DATA nulls and clears delay",
          rows[5]["stop_status"] == "no_data"
          and rows[5]["null_reason"] == "no_data"
          and rows[9]["predicted_arrival"] is None
          and rows[9]["null_reason"] == "no_trust")
    check("rt/trust rebuilt after NO_DATA",
          rows[20]["predicted_arrival"] == sched[20][0] - 15
          and rows[20]["arrival_basis"] == "explicit"
          and rows[20]["predicted_departure"] == sched[20][1] - 15)

    # 7) arrival/departure distinct values; departure updates downstream delay.
    with warehouse("arrdep") as live:
        live.apply(build_feed([{
            "id": "e7",
            "key": LOOP_KEY,
            "stops": [
                {"stop_sequence": 0,
                 "arrival": {"delay": 60}, "departure": {"delay": 300}},
            ],
        }]))
        rows = _by_sequence(live.query())
        check("rt/departure value sets the downstream delay",
              rows[0]["predicted_arrival"] == sched[0][0] + 60
              and rows[0]["arrival_src_event"] == "arrival"
              and rows[0]["predicted_departure"] == sched[0][1] + 300
              and rows[0]["departure_basis"] == "explicit"
              and rows[5]["predicted_arrival"] == sched[5][0] + 300
              and rows[5]["arrival_src_event"] == "departure"
              and rows[5]["arrival_src_sequence"] == 0)
        # departure missing on a later stop keeps the post-arrival delay.
        live.apply(build_feed([{
            "id": "e7b",
            "key": LOOP_KEY,
            "stops": [
                {"stop_sequence": 0, "arrival": {"delay": 60}},
                {"stop_sequence": 5, "arrival": {"delay": 75}},
            ],
        }], timestamp=2))
        rows = _by_sequence(live.query())
        check("rt/missing departure reuses post-arrival delay",
              rows[5]["predicted_departure"] == sched[5][1] + 75
              and rows[5]["departure_src_event"] == "arrival"
              and rows[5]["departure_src_sequence"] == 5)

    # 8) CANCELED: whole instance null/canceled, no stop updates allowed.
    with warehouse("cancel") as live:
        live.apply(build_feed(
            [{"id": "e8", "key": LOOP_KEY,
              "trip_relationship": "CANCELED"}]))
        rows = live.query()
    check("rt/CANCELED marks every stop and nulls predictions",
          all(r["stop_status"] == "canceled" for r in rows)
          and all(r["null_reason"] == "canceled" for r in rows)
          and all(r["predicted_arrival"] is None for r in rows)
          and all(r["entity_id"] == "e8" for r in rows))

    run_strict_subset_tests(work, catalog)

def run_strict_subset_tests(work, catalog):
    def expect_reject(name, payload):
        safe = name.replace("/", "-")
        with LiveArrivals(work / f"strict-{safe}.sqlite", catalog) as live:
            try:
                live.apply(payload)
                check(name, False, "accepted unsupported field")
            except LiveFeedError:
                check(name, True)

    expect_reject("strict/route_id", build_feed(
        [{"id": "e", "key": LOOP_KEY, "route_id": "R1",
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
        timestamp=1))
    expect_reject("strict/TripUpdate.delay", build_feed(
        [{"id": "e", "key": LOOP_KEY, "trip_delay": 5,
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
        timestamp=1))
    expect_reject("strict/uncertainty", build_feed(
        [{"id": "e", "key": LOOP_KEY,
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1},
                     "uncertainty": 30}]}], timestamp=1))
    expect_reject("strict/ADDED relationship", build_feed(
        [{"id": "e", "key": LOOP_KEY, "trip_relationship": "ADDED",
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
        timestamp=1))
    expect_reject("strict/UNSCHEDULED stop", build_feed(
        [{"id": "e", "key": LOOP_KEY,
          "stops": [{"stop_sequence": 0, "relationship": "UNSCHEDULED"}]}],
        timestamp=1))
    expect_reject("strict/unknown trip instance", build_feed(
        [{"id": "e", "key": ("T", "20260101", "09:00:00"),
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
        timestamp=1))
    expect_reject("strict/unknown stop_sequence", build_feed(
        [{"id": "e", "key": LOOP_KEY,
          "stops": [{"stop_sequence": 99, "arrival": {"delay": 1}}]}],
        timestamp=1))
    expect_reject("strict/non-increasing sequences", build_feed(
        [{"id": "e", "key": LOOP_KEY, "stops": [
            {"stop_sequence": 5, "arrival": {"delay": 1}},
            {"stop_sequence": 5, "arrival": {"delay": 2}}]}], timestamp=1))
    expect_reject("strict/SKIPPED with event", build_feed(
        [{"id": "e", "key": LOOP_KEY, "stops": [
            {"stop_sequence": 0, "relationship": "SKIPPED",
             "arrival": {"delay": 1}}]}], timestamp=1))
    expect_reject("strict/CANCELED with stops", build_feed(
        [{"id": "e", "key": LOOP_KEY, "trip_relationship": "CANCELED",
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
        timestamp=1))
    expect_reject("strict/wrong version", build_feed(
        [{"id": "e", "key": LOOP_KEY,
          "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
        timestamp=1, version="1.0"))
    expect_reject("strict/garbage bytes", b"\xff\xff\xff\xff")
    expect_reject("strict/delete in FULL feed", build_feed(
        [{"id": "e", "is_deleted": True}], timestamp=1))

    # A genuinely unknown wire field (field 15 inside a StopTimeUpdate).
    stu = gtfs.TripUpdate.StopTimeUpdate()
    stu.stop_sequence = 0
    stu.arrival.delay = 10
    raw = stu.SerializeToString() + b"\x78\x05"
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs.FeedHeader.FULL_DATASET
    feed.header.timestamp = 1
    entity = feed.entity.add()
    entity.id = "e"
    entity.trip_update.trip.trip_id = "T"
    entity.trip_update.trip.start_date = "20260101"
    entity.trip_update.trip.start_time = "08:00:30"
    entity.trip_update.stop_time_update.add().ParseFromString(raw)
    check("strict/unknown wire field really unknown",
          len(unknown_fields.UnknownFieldSet(
              entity.trip_update.stop_time_update[0])) == 1)
    expect_reject("strict/unknown wire field", feed.SerializeToString())


# --------------------------------------------------------------------------- #
# Feed lifecycle, durability, rollback
# --------------------------------------------------------------------------- #
def run_lifecycle_tests(work):
    # Two service-day instances of the same trip so two entities can be active
    # simultaneously without claiming the same instance.
    source = work / "life-src"
    write_tables(
        source,
        trips=["T"],
        stop_times=[
            st("T", 0, "A", "08:00:00", "08:00:00"),
            st("T", 5, "B", "08:10:00", "08:11:00"),
            st("T", 9, "A", "08:20:00", "08:20:30"),
            st("T", 20, "C", "08:30:00", "08:30:30"),
        ],
        instances=[
            inst("T", "20260101", "08:00:30"),
            inst("T", "20260102", "08:00:30"),
        ],
    )
    catalog = work / "life-cat"
    TimetableService(catalog).publish(source)
    d1 = day_epoch("20260101")
    d2 = day_epoch("20260102")
    # First departure is 08:00:00 wall time but the instance start_time is
    # 08:00:30, so every call shifts +30s from the raw template offsets.
    shift = parse_hms("08:00:30") - parse_hms("08:00:00")
    b1 = d1 + parse_hms("08:00:00") + shift
    sched1 = {
        0: (b1, b1),
        5: (b1 + 600, b1 + 660),
        9: (b1 + 1200, b1 + 1230),
        20: (b1 + 1800, b1 + 1830),
    }
    sched2 = {seq: (a + (d2 - d1), b + (d2 - d1))
              for seq, (a, b) in sched1.items()}
    key1 = ("T", "20260101", "08:00:30")
    key2 = ("T", "20260102", "08:00:30")
    db = work / "life.sqlite"

    full1 = build_feed([{
        "id": "alpha", "key": key1,
        "stops": [{"stop_sequence": 0, "arrival": {"delay": 100}}],
    }], timestamp=100)
    diff1 = build_feed([{
        "id": "beta", "key": key2,
        "stops": [{"stop_sequence": 5, "arrival": {"delay": 200}}],
    }], timestamp=101, incrementality="DIFFERENTIAL")
    diff_delete = build_feed(
        [{"id": "alpha", "is_deleted": True}],
        timestamp=103, incrementality="DIFFERENTIAL")
    full2 = build_feed([{
        "id": "gamma", "key": key1, "trip_relationship": "CANCELED",
    }], timestamp=104)

    with LiveArrivals(db, catalog) as live:
        live.apply(full1)
        live.apply(diff1)
        day1 = _by_sequence(live.query(instance=key1))
        day2 = _by_sequence(live.query(instance=key2))
        check("life/differential adds a second active entity",
              day1[0]["predicted_arrival"] == sched1[0][0] + 100
              and day2[5]["predicted_arrival"] == sched2[5][0] + 200
              and day2[0]["predicted_arrival"] is None
              and {e["entity_id"] for e in
                   live.inspect()["active_entities"]} == {"alpha", "beta"})

        # A second entity pointing at day1 conflicts -> whole feed rejected.
        try:
            live.apply(build_feed(
                [{"id": "beta2", "key": key1,
                  "stops": [{"stop_sequence": 9, "arrival": {"delay": 5}}]}],
                timestamp=102, incrementality="DIFFERENTIAL"))
            check("life/conflicting entity rejects feed", False, "accepted")
        except LiveFeedError:
            check("life/conflicting entity rejects feed", True)
        check("life/rejected conflict leaves prior predictions intact",
              live.inspect()["last_feed"]["timestamp"] == 101
              and {e["entity_id"] for e in
                   live.inspect()["active_entities"]} == {"alpha", "beta"})

        # idempotent same-bytes replay and timestamp rules.
        replay = live.apply(diff1)
        check("life/same bytes same timestamp is idempotent",
              replay["idempotent_replay"] is True)
        try:
            live.apply(build_feed(
                [{"id": "other", "key": key1,
                  "stops": [{"stop_sequence": 0, "arrival": {"delay": 1}}]}],
                timestamp=101))
            check("life/different bytes same timestamp rejected", False, "ok")
        except LiveFeedError:
            check("life/different bytes same timestamp rejected", True)
        try:
            live.apply(build_feed([], timestamp=99))
            check("life/older timestamp rejected", False, "ok")
        except LiveFeedError:
            check("life/older timestamp rejected", True)

        # Deleting an unknown id is an idempotent no-op.
        live.apply(build_feed(
            [{"id": "ghost", "is_deleted": True}],
            timestamp=102, incrementality="DIFFERENTIAL"))
        check("life/delete unknown id is idempotent no-op",
              live.inspect()["last_feed"]["timestamp"] == 102
              and {e["entity_id"] for e in
                   live.inspect()["active_entities"]} == {"alpha", "beta"})

        # Delete alpha: day1 returns to purely static, day2 keeps beta.
        live.apply(diff_delete)
        day1 = _by_sequence(live.query(instance=key1))
        day2 = _by_sequence(live.query(instance=key2))
        check("life/delete restores old instance to static state",
              all(r["predicted_arrival"] is None for r in day1.values())
              and all(r["entity_id"] is None for r in day1.values())
              and day2[5]["predicted_arrival"] == sched2[5][0] + 200)

        # New FULL wipes both entities; the canceled day1 replaces everything.
        live.apply(full2)
        rows = live.query()
        check("life/FULL replaces whole active set",
              live.inspect()["active_entities"] == [{
                  "entity_id": "gamma",
                  "trip_id": "T", "start_date": "20260101",
                  "start_time": "08:00:30",
                  "schedule_relationship": "CANCELED",
              }]
              and all(r["stop_status"] == "canceled"
                      for r in rows if (r["start_date"], r["start_time"])
                      == ("20260101", "08:00:30"))
              and all(r["entity_id"] is None
                      for r in rows if r["start_date"] == "20260102"))
        feeds = live.accepted_feeds()
        check("life/feed log records every accepted feed",
              [f["feed_timestamp"] for f in feeds]
              == [100, 101, 102, 103, 104])

    # Reopen: durable entities re-explain the view from SQLite alone.
    with LiveArrivals(db, catalog) as reopened:
        rows = reopened.query(instance=key1)
        check("life/reopen re-explains active entities and predictions",
              all(r["stop_status"] == "canceled" for r in rows)
              and reopened.inspect()["last_feed"]["timestamp"] == 104)

    # Entity re-pointing: old instance must lose all realtime state.
    second_source = work / "two-src"
    write_tables(
        second_source, ["T"],
        [st("T", 1, "A", "08:00:00", "08:00:00"),
         st("T", 2, "B", "08:10:00", "08:10:00")],
        [inst("T", "20260101", "08:00:00"),
         inst("T", "20260103", "08:00:00")],
    )
    cat_two = work / "two-cat"
    TimetableService(cat_two).publish(second_source)
    db2 = work / "repoint.sqlite"
    with LiveArrivals(db2, cat_two) as live:
        live.apply(build_feed(
            [{"id": "e", "key": ("T", "20260101", "08:00:00"),
              "stops": [{"stop_sequence": 1, "arrival": {"delay": 40}}]}],
            timestamp=1))
        live.apply(build_feed(
            [{"id": "e", "key": ("T", "20260103", "08:00:00"),
              "stops": [{"stop_sequence": 1, "arrival": {"delay": 40}}]}],
            timestamp=2, incrementality="DIFFERENTIAL"))
        first = _by_sequence(live.query(instance=("T", "20260101", "08:00:00")))
        check("life/repointed entity leaves old instance purely static",
              all(r["entity_id"] is None for r in first.values())
              and all(r["predicted_arrival"] is None for r in first.values()))

    # Bad last entity: no half-feed predictions, state stays at previous feed.
    db3 = work / "bad-last.sqlite"
    with LiveArrivals(db3, catalog) as live:
        live.apply(build_feed(
            [{"id": "good", "key": key1,
              "stops": [{"stop_sequence": 0, "arrival": {"delay": 11}}]}],
            timestamp=1))
        try:
            live.apply(build_feed(
                [{"id": "good", "key": key1,
                  "stops": [{"stop_sequence": 0, "arrival": {"delay": 11}}]},
                 {"id": "bad", "key": key1,
                  "stops": [{"stop_sequence": 999,
                             "arrival": {"delay": 1}}]}],
                timestamp=2))
            check("life/bad last entity rolls back", False, "accepted")
        except LiveFeedError:
            check("life/bad last entity rolls back", True)
        rows = _by_sequence(live.query(instance=key1))
        check("life/rollback keeps previous complete feed",
              rows[0]["predicted_arrival"] == sched1[0][0] + 11
              and live.inspect()["last_feed"]["timestamp"] == 1)

    # Direct SQLite read: predictions table really holds the committed view.
    raw_db = sqlite3.connect(db3)
    raw_db.row_factory = sqlite3.Row
    committed = [dict(r) for r in raw_db.execute(
        "SELECT stop_sequence, predicted_arrival, arrival_basis, entity_id "
        "FROM predictions ORDER BY stop_sequence")]
    raw_db.close()
    check("life/predictions are directly readable in SQLite",
          committed[0]["predicted_arrival"] == sched1[0][0] + 11
          and committed[0]["arrival_basis"] == "explicit"
          and committed[0]["entity_id"] == "good")

    # Warehouse binds one fixed catalog; a different catalog id is refused.
    try:
        LiveArrivals(db3, cat_two)
        check("life/rebinding to another catalog is refused", False, "opened")
    except LiveFeedError:
        check("life/rebinding to another catalog is refused", True)

    # Catalog directory from "another publisher" (plain path, no service obj).
    with LiveArrivals(work / "path-only.sqlite", str(catalog)) as live:
        live.apply(build_feed(
            [{"id": "p", "key": key1,
              "stops": [{"stop_sequence": 0, "arrival": {"delay": 7}}]}],
            timestamp=1))
        check("life/catalog accepted as plain directory path",
              live.query(instance=key1)[0]["predicted_arrival"]
              == sched1[0][0] + 7)


if __name__ == "__main__":
    main()
