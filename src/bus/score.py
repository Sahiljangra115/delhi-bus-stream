"""Real-time scorer: Kafka -> features -> Feast online store -> model.

Per poll batch: update features, write the latest row per vehicle to the Feast
online store, READ the features back from Feast (serving path), predict ETA to
next stop and bunching probability. Latency is wall clock from the poller's
publish time (ingest_ts) to the end of the batch's prediction.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from . import fs
from .features import FEATURES, FeatureEngine
from .sim import load_gtfs
from .stream import make_consumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="vehicle_positions")
    ap.add_argument("--group", default=f"score-{int(time.time())}")
    ap.add_argument("--gtfs", default="data/gtfs")
    ap.add_argument("--data", default="data")
    ap.add_argument("--max-batch", type=int, default=200)
    ap.add_argument("--idle", type=float, default=10.0)
    ap.add_argument("--tag", default="live")
    ap.add_argument("--poll-timeout", type=float, default=0.05,
                    help="max wait for a batch to fill; trades latency against batch size")
    a = ap.parse_args()
    # small batches: multi-threaded OpenMP predict is slower, and collapses when the CPU is shared
    threadpool_limits(1)
    d = Path(a.data)
    mdl = joblib.load(d / "model.joblib")
    net = load_gtfs(Path(a.gtfs))
    eng = FeatureEngine(net.routes, net.trips, net.headway)
    store = fs.apply()
    refs = fs.refs(FEATURES)
    c = make_consumer(a.bootstrap, a.group)
    c.subscribe([a.topic])
    lat, stage, log = [], {"feat": [], "write": [], "read": [], "pred": []}, []
    first = last = None
    lags, next_lag = [], 0.0
    n_msgs = n_scored = 0
    idle_since = time.time()
    while time.time() - idle_since < (a.idle if first else 300):
        msgs = c.consume(a.max_batch, timeout=a.poll_timeout)
        if first and time.time() >= next_lag:     # sample consumer lag once a second
            next_lag = time.time() + 1.0
            lag = 0
            for tp in c.assignment():
                # cached=True: high watermark from the last fetch response, no broker round
                # trip. cached=False blocked about 0.15-0.5 s per partition under overload,
                # which throttled the scorer to about one batch per 2 s (bug in the 10-14 run).
                lo, hi = c.get_watermark_offsets(tp, cached=True)
                if hi < 0:
                    continue
                pos = c.position([tp])[0].offset
                lag += hi - (pos if pos >= 0 else lo)
            lags.append((round(time.time() - first, 1), lag))
        if not msgs:
            continue
        idle_since = time.time()
        first = first or time.time()
        t0 = time.time()
        rows, ing = {}, {}
        for m in msgs:
            if m.error():
                continue
            j = json.loads(m.value())
            n_msgs += 1
            row, _ = eng.update(j)
            if row:
                rows[row["vehicle_id"]] = row   # keep latest per vehicle in this batch
                ing[row["vehicle_id"]] = j["ingest_ts"]
        c.commit(asynchronous=True)   # committed offsets make lag visible to bus.monitor
        if not rows:
            continue
        t1 = time.time()
        df = pd.DataFrame(list(rows.values()))
        df["event_timestamp"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        store.write_to_online_store("vehicle_stats", df[["vehicle_id", "event_timestamp"] + FEATURES])
        t2 = time.time()
        on = store.get_online_features(
            features=refs, entity_rows=[{"vehicle_id": v} for v in df["vehicle_id"]]).to_df()
        t3 = time.time()
        X = on[FEATURES]
        eta = mdl["reg"].predict(X)
        pb = mdl["clf"].predict_proba(X)[:, 1]
        t4 = time.time()
        last = t4
        n_scored += len(df)
        lat.extend(t4 - np.array([ing[v] for v in df["vehicle_id"]]))
        stage["feat"].append(t1 - t0); stage["write"].append(t2 - t1)
        stage["read"].append(t3 - t2); stage["pred"].append(t4 - t3)
        on = on.set_index("vehicle_id")
        for v, r, e_, b_ in zip(df["vehicle_id"], df.to_dict("records"), eta, pb):
            log.append(dict(vehicle_id=v, ts=r["ts"], pred_eta_s=float(e_), p_bunch=float(b_),
                            **{f"eng_{f}": r[f] for f in FEATURES},
                            **{f"on_{f}": on.loc[v, f] for f in FEATURES}))
    c.close()
    pd.DataFrame(log).to_parquet(d / f"{a.tag}_log.parquet")
    q = lambda x, p: round(float(np.quantile(x, p)) * 1000, 1)
    span = (last - first) if first and last else float("nan")
    print(json.dumps(dict(
        tag=a.tag, messages=n_msgs, scored=n_scored, batches=len(stage["feat"]),
        wall_s=round(span, 2), msgs_per_s=round(n_msgs / span, 1),
        scored_per_s=round(n_scored / span, 1),
        latency_ms=dict(p50=q(lat, .5), p95=q(lat, .95), p99=q(lat, .99), max=q(lat, 1)),
        mean_stage_ms={k: round(1000 * float(np.mean(v)), 2) for k, v in stage.items()},
        engine_stats=eng.stats,
        max_lag_msgs=max((l for _, l in lags), default=0),
        lag_samples=lags[:: max(1, len(lags) // 12)])))


if __name__ == "__main__":
    main()
