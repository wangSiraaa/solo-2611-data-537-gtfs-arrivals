# Scheduled calls and live arrivals

Python 3.11. Install `requirements.txt` into a virtualenv, then run from
`materials/537`:

```
python demo.py      # three-stop trip, +120s arrival delay, full provenance
python verify.py    # 66 independent checks with hand-computed expectations
```

## Two independent services

### `TimetableService` — static timetable instance service

Reads GTFS-shaped `trips.txt` and `stop_times.txt` plus an explicit
`instances.txt` that lists `(trip_id, start_date, start_time)` keys (no
calendar/frequencies expansion). It publishes a self-contained
**`scheduled-calls/v1`** directory:

* `calls.csv` — one row per scheduled call with the full instance key,
  `stop_sequence`, `stop_id`, planned `arrival`/`departure` as **UTC integer
  seconds**, and the `source_row` of the originating `stop_times.txt` line;
* `manifest.json` — schema, counts, source path and a SHA-256 of `calls.csv`.

Times follow the GTFS wall-clock convention with HH in 0..47 and a fixed
UTC+08 zone: values are **never reduced modulo 24**, so `24:10:00` stays on the
same service day at 87000 seconds. Each instance shifts every template stop by
the offset of its own `start_time` from the template's first departure, then
adds the service-day midnight epoch. Crossing midnight does not change the
instance identity.

Validation is all-or-nothing: unknown trip, duplicate trip, duplicate full
instance key, duplicate `stop_sequence` within a trip, illegal time/date or a
non-monotonic template reject the whole publish and leave any previously
published directory byte-for-byte untouched (atomic file replacement).

* `lookup(trip_id, start_date, start_time)` reads one exact instance key.
* `query(stop_id, lo, hi)` returns scheduled departures in `[lo, hi)`,
  including every repeated visit to the same `stop_id`.

A static client only needs `CatalogReader` (or `TimetableService`) and the
on-disk directory — it never starts the live warehouse.

### `LiveArrivals` — real-time arrivals warehouse

`LiveArrivals(database, timetable_or_catalog_path)` binds at construction to
**one fixed published catalog directory**, read only through `CatalogReader`.
It never holds a publisher object or private callback; a catalog from another
publisher is accepted as long as it matches the documented row format and
ordering rules. The catalog is snapshotted into SQLite, and reopening the
database re-explains every active entity and prediction from durable state.

The feed is a standard `gtfs-realtime-bindings` `FeedMessage` using a strict,
explicitly listed subset. Any other **known** field
(`route_id`, `direction_id`, `TripUpdate.delay`, `StopTimeEvent.uncertainty`,
`vehicle`, …) and any genuinely unknown wire field reject the whole feed rather
than being silently ignored. Only `FULL_DATASET`/`DIFFERENTIAL`, trip
`SCHEDULED`/`CANCELED`, and stop `SCHEDULED`/`SKIPPED`/`NO_DATA` are supported;
vehicle positions and alerts are not.

#### Projection rules

Stops are walked from the first one, carrying at most one trusted delay.

* A `StopTimeUpdate` selects by `stop_sequence`; a matching `stop_id` must
  agree. With only `stop_id`, the id must be unique on the instance — a loop
  line's repeated stop cannot be picked arbitrarily. Resolved sequences must be
  strictly increasing with no repeated stop.
* An event carries optional `time` and `delay`. Explicit `delay = 0` is a real
  prediction, distinct from an absent field. When both appear, **`time` wins**
  and re-bases the carried delay as `time - scheduled` for that field.
* Without an explicit value, a field is predicted only while a trusted delay
  exists, by shifting the scheduled instant. The departure of a stop defaults
  to the delay established after its arrival; an explicit departure value
  becomes the delay used downstream.
* `SKIPPED` stops carry no events, have both predictions null and keep the
  current delay flowing to later stops.
* `NO_DATA` stops carry no events and **clear** the trusted delay; later stops
  stay null until a new explicit `time`/`delay` rebuilds trust.
* `CANCELED` trips carry no stop updates; every stop is `canceled` with null
  predictions. Negative delays are legal and predicted times are not forced to
  be monotonic.

Every predicted column records its basis — `explicit` vs `propagated` — plus
the exact source `stop_sequence` and event kind (`arrival`/`departure`) it came
from, so an audit can explain each value independently rather than via one
final total delay. Nulls carry a reason: `no_realtime` (no active entity),
`no_trust`, `skipped`, `no_data`, or `canceled`.

#### Visibility, transactions and the chosen rebuild strategy

The warehouse stores the **active entities** (their serialized protobuf) and
rebuilds the whole small prediction view from those entities plus the static
snapshot on every accepted feed. This makes whole-feed visibility trivial to
reason about:

* a `FULL_DATASET` replaces the entire active set, so entities it omits vanish;
* a `DIFFERENTIAL` upserts the listed entities, deletes named ids (deleting an
  unknown id is an idempotent no-op), and leaves the rest untouched;
* **deleting an entity, or re-pointing it at another instance, simply removes
  its old instance key from the rebuilt projection**, so that instance returns
  to its purely static rows with no leftover cancellation or delay;
* a `NO_DATA` cut is recomputed the same way during every rebuild, so the
  absence of trust never depends on stale per-stop state carried over from a
  previous feed.

The feed is fully validated and the candidate view fully projected **before**
any write. One SQLite transaction then swaps the active entities, prediction
view and feed state together: a bad last entity, bad catalog reference or
failing stop validation cannot leave half a prediction behind, and a
pre-commit exception rolls back to the previous complete feed. Feed timestamps
move strictly forward; equal-timestamp, byte-identical replays are idempotent,
equal-timestamp different bytes and older feeds are rejected.

## Command line

```
python cli.py publish --source DIR --catalog DIR
python cli.py lookup  --catalog DIR --trip T --date 20260101 --time 08:00:00
python cli.py squery  --catalog DIR --stop A --lo EPOCH --hi EPOCH
python cli.py apply   --db FILE --catalog DIR --feed FILE.pb
python cli.py replay  --db FILE --catalog DIR --feed-dir DIR   # sorted *.pb
python cli.py query   --db FILE --catalog DIR [--stop A] [--lo N] [--hi N]
                                 [--trip T --date 20260101 --time 08:00:00]
python cli.py inspect --db FILE --catalog DIR
```

`inspect` reports the bound catalog identity (path + SHA-256), every accepted
feed (timestamp, incrementality, entity count, payload hash, applied time), the
active entities and the full arrivals view including each prediction's basis
and source.
