"""Kafka consumer -> FeatureEngine -> offline feature store (parquet) + labels.

This is the batch-ish "history builder": it drains the topic and writes
features.parquet (Feast offline source) and labels.parquet.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd

from .features import FEATURES, FeatureEngine
from .sim import load_gtfs


def consume(consumer, topic, handle, idle_s=8.0, max_batch=500):
    """Poll until the topic is idle for idle_s. handle(msg_dict) per record.

    Returns (records, busy seconds from first to last record), so group join
    time and the idle wait are not counted as processing time.
    """
    consumer.subscribe([topic])
    last, n, first = time.time(), 0, None
    while time.time() - last < idle_s:
        msgs = consumer.consume(max_batch, timeout=1.0)
        first = first or (time.time() if msgs else None)
        for m in msgs:
            if m.error():
                continue
            handle(json.loads(m.value()))
            n += 1
        if msgs:
            last = time.time()
            consumer.commit(asynchronous=True)   # named group + committed offsets = measurable lag
    return n, (last - first) if first else 0.0


def make_consumer(bootstrap, group, reset="earliest"):
    from confluent_kafka import Consumer
    return Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                     "auto.offset.reset": reset, "enable.auto.commit": False})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="vehicle_positions")
    ap.add_argument("--group", default=f"history-{int(time.time())}")
    ap.add_argument("--gtfs", default="data/gtfs")
    ap.add_argument("--out", default="data")
    a = ap.parse_args()
    net = load_gtfs(Path(a.gtfs))
    eng = FeatureEngine(net.routes, net.trips, net.headway)
    feats, labels, raw = [], [], []

    def handle(m):
        raw.append({k: m[k] for k in ('vehicle_id', 'trip_id', 'lat', 'lon', 'timestamp')})
        row, lab = eng.update(m)
        if row:
            feats.append(row)
        labels.extend(lab)

    c = make_consumer(a.bootstrap, a.group)
    n, dt = consume(c, a.topic, handle)
    try:
        c.commit(asynchronous=False)
    except Exception:   # nothing consumed: no offsets to commit
        pass
    c.close()
    f = pd.DataFrame(feats)
    f["event_timestamp"] = pd.to_datetime(f["ts"], unit="s", utc=True)
    f[["vehicle_id", "event_timestamp"] + FEATURES].to_parquet(Path(a.out) / "features.parquet")
    pd.DataFrame(raw).to_parquet(Path(a.out) / "raw_pings.parquet")
    lab = pd.DataFrame(labels)
    lab["event_timestamp"] = pd.to_datetime(lab["ts"], unit="s", utc=True)
    lab.drop(columns=["_progress"]).to_parquet(Path(a.out) / "labels.parquet")
    print(json.dumps(dict(consumed=n, feature_rows=len(f), labelled=len(lab),
                          stats=eng.stats, seconds=round(dt, 2),
                          events_per_s=round(n / max(dt, 1e-9), 1))))


if __name__ == "__main__":
    main()
