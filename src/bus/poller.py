"""Poll a GTFS-realtime VehiclePositions source and publish to Kafka.

Sources:
  otd : the real Delhi OTD feed. Needs OTD_API_KEY in the environment. Without
        it this exits with an error. It never falls back silently to fake data.
  sim : the SYNTHETIC simulator (src/bus/sim.py). Every message carries
        source="SIMULATED" so downstream data can never be mistaken for live.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from google.protobuf.message import DecodeError

from . import gtfsrt
from .sim import Simulator, build_network, load_gtfs

OTD_URL = "https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb"


def otd_snapshots(poll_s=10):
    key = os.environ.get("OTD_API_KEY")
    if not key:
        sys.exit("OTD_API_KEY not set: cannot poll the real feed. "
                 "Register at https://otd.delhi.gov.in, or use --source sim (SYNTHETIC data).")
    import requests
    while True:
        t = time.time()
        try:   # one bad poll must not end a long collection run
            r = requests.get(OTD_URL, params={"key": key}, timeout=10)
            r.raise_for_status()
            yield r.content
        except requests.RequestException as e:
            print(f"poll failed: {type(e).__name__} {getattr(e.response, 'status_code', '')}",
                  file=sys.stderr)
        time.sleep(max(0, poll_s - (time.time() - t)))


def sim_snapshots(gtfs_dir, t0, t1, seed, speedup, truth_out=None):
    """Yield protobuf snapshots; speedup=0 means as fast as possible."""
    net = load_gtfs(Path(gtfs_dir))
    sim = Simulator(net, t0, t1, seed=seed)
    wall0 = time.time()
    for T, ents in sim.run():
        if speedup:
            delay = (T - t0) / speedup - (time.time() - wall0)
            if delay > 0:
                time.sleep(delay)
        yield gtfsrt.encode(ents, T)
    if truth_out:   # ground truth arrivals, only for validating labels offline
        with open(truth_out, "w") as f:
            f.write("trip_id,stop_idx,arrival_ts\n")
            for (tr, k), v in sim.truth.items():
                f.write(f"{tr},{k},{v:.3f}\n")


def make_producer(bootstrap, linger_ms=5):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap, "linger.ms": linger_ms,
                     "compression.type": "lz4", "queue.buffering.max.messages": 500000})


def publish(producer, topic, buf, source, seen):
    """Decode one snapshot, drop stale repeats, publish the rest. Returns count."""
    _, ents = gtfsrt.decode(buf)
    n, now = 0, time.time()
    for e in ents:
        k = (e["vehicle_id"], e["timestamp"])
        if k in seen:
            continue
        seen.add(k)
        e["ingest_ts"] = now
        e["source"] = source
        while True:
            try:
                producer.produce(topic, json.dumps(e).encode(), key=e["route_id"].encode())
                break
            except BufferError:   # local queue full: block until the broker drains it
                producer.poll(0.1)
        n += 1
    producer.poll(0)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["otd", "sim"], required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="vehicle_positions")
    ap.add_argument("--gtfs", default="data/gtfs")
    ap.add_argument("--t0", type=float, default=6 * 3600)
    ap.add_argument("--t1", type=float, default=14 * 3600)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--truth-out", default=None)
    ap.add_argument("--speedup", type=float, default=0, help="sim only; 0 = max speed")
    ap.add_argument("--duration", type=float, default=0, help="stop after N seconds; 0 = run forever")
    ap.add_argument("--raw-out", default=None,
                    help="parquet of every entity in every snapshot, before dedup (feed quality analysis)")
    a = ap.parse_args()
    p = make_producer(a.bootstrap)
    snaps = (otd_snapshots() if a.source == "otd"
             else sim_snapshots(a.gtfs, a.t0, a.t1, a.seed, a.speedup, a.truth_out))
    label = "LIVE" if a.source == "otd" else "SIMULATED"
    seen, total, t, raw, bad, polls = set(), 0, time.time(), [], 0, 0
    try:
        for buf in snaps:
            polls += 1
            try:
                hts, ents = gtfsrt.decode(buf)
            except DecodeError:   # truncated or non-protobuf body: skip this poll
                bad += 1
                print("corrupt snapshot skipped", file=sys.stderr)
                continue
            if a.raw_out:
                now = time.time()
                raw += [dict(poll_ts=now, header_ts=hts, **e) for e in ents]
            total += publish(p, a.topic, buf, label, seen)
            if a.duration and time.time() - t > a.duration:
                break
    finally:   # keep what was collected even if the run dies
        p.flush()
        if a.raw_out and raw:
            import pandas as pd
            pd.DataFrame(raw).assign(source=label).to_parquet(a.raw_out)
    dt = time.time() - t
    print(json.dumps(dict(source=label, published=total, polls=polls, corrupt_polls=bad,
                          seconds=round(dt, 2), msgs_per_s=round(total / dt, 1))))


if __name__ == "__main__":
    main()
