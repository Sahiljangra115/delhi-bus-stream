"""Kafka-only throughput numbers (no features). Produce N prebuilt messages,
then consume them back. Reports msgs/s for each side."""
import argparse
import json
import time

import pandas as pd

from .poller import make_producer
from .stream import make_consumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="bench")
    ap.add_argument("--raw", default="data/raw_pings.parquet")
    ap.add_argument("--n", type=int, default=200000)
    a = ap.parse_args()
    raw = pd.read_parquet(a.raw)
    raw = pd.concat([raw] * (a.n // len(raw) + 1)).head(a.n)
    msgs = [(json.dumps(dict(vehicle_id=r.vehicle_id, trip_id=r.trip_id, lat=r.lat, lon=r.lon,
                             timestamp=int(r.timestamp), ingest_ts=0.0)).encode(),
             r.trip_id.split("_")[0].encode()) for r in raw.itertuples()]
    p = make_producer(a.bootstrap)
    t = time.time()
    for v, k in msgs:
        while True:
            try:
                p.produce(a.topic, v, key=k)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
    p.flush()
    tp = time.time() - t
    c = make_consumer(a.bootstrap, f"bench-{int(time.time())}")
    c.subscribe([a.topic])
    n, t, idle = 0, None, time.time()
    while n < len(msgs) and time.time() - idle < 10:
        b = c.consume(5000, timeout=1.0)
        if b:
            if t is None:   # clock starts after the first batch, not at group join
                t, n0 = time.time(), len(b)
            n += len(b)
            idle = time.time()
    tc = time.time() - t
    print(json.dumps(dict(n=len(msgs), bytes_per_msg=len(msgs[0][0]), produce_msgs_per_s=round(len(msgs) / tp),
                          consumed=n, consume_msgs_per_s=round((n - n0) / tc))))


if __name__ == "__main__":
    main()
