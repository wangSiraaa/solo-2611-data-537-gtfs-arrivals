# Scheduled calls and live arrivals

Python 3.11. Install `requirements.txt`, then run:

```
python materials/537/demo.py
```

The publisher can produce/query a scheduled catalog without starting the live
service. The live service reads its catalog through `TimetableService.lookup`;
a catalog supplied by another publisher can use the same documented row shape.
GTFS-RT input is protobuf, using the standard generated message classes.

The current starter handles a simple full feed with a single trip instance,
unique stops, same-day times and arrival delays. It is an ordinary prototype,
not a complete implementation of the task contract.
