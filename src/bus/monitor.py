"""Pipeline monitor: appends one row per interval to a CSV.

Columns: wall_ts, msgs_total, msg_rate, lag (sum over partitions for --group, -1 if the
group has no committed offsets), vehicles_seen, stale_frac.

stale_frac = share of active vehicles whose newest ping is older than --stale-s relative to
the newest ping seen on the stream (so it works for sim time too). Vehicles silent for more
than --gone-s count as gone (trip ended) and leave the denominator. The monitor uses its own
group and never commits, so it does not disturb real consumers.
"""
from __future__ import annotations

import argparse
import csv
import json
import signal
import time
from pathlib import Path


def main():
    from confluent_kafka import Consumer, TopicPartition
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="live")
    ap.add_argument("--group", required=True, help="consumer group whose lag is reported")
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--stale-s", type=float, default=120.0)
    ap.add_argument("--gone-s", type=float, default=600.0)
    ap.add_argument("--duration", type=float, default=0, help="0 = until Ctrl-C")
    ap.add_argument("--out", default="data/monitor.csv")
    a = ap.parse_args()

    def _stop(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _stop)   # background jobs in sh ignore SIGINT

    # probe consumer reads the stream tail for staleness; lag consumer only queries offsets
    probe = Consumer({"bootstrap.servers": a.bootstrap, "group.id": f"monitor-probe-{int(time.time())}",
                      "auto.offset.reset": "latest", "enable.auto.commit": False})
    probe.subscribe([a.topic])
    meta = Consumer({"bootstrap.servers": a.bootstrap, "group.id": a.group, "enable.auto.commit": False})
    parts = [TopicPartition(a.topic, p) for p in meta.list_topics(a.topic, timeout=10).topics[a.topic].partitions]

    last_ts, newest = {}, 0.0
    prev_total, prev_t = None, time.time()
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    new = not out.exists()
    t_end = time.time() + a.duration if a.duration else None
    with open(out, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["wall_ts", "msgs_total", "msg_rate", "lag", "vehicles_seen", "stale_frac"])
        try:
            while t_end is None or time.time() < t_end:
                tick = time.time() + a.interval
                while time.time() < tick:   # drain the tail until next sample
                    m = probe.poll(0.2)
                    if m is None or m.error():
                        continue
                    e = json.loads(m.value())
                    last_ts[e["vehicle_id"]] = e["timestamp"]
                    newest = max(newest, e["timestamp"])
                total = lag = 0
                committed = meta.committed(parts, timeout=10)
                has_commit = False
                for tp in committed:
                    lo, hi = meta.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)
                    total += hi - lo
                    if tp.offset >= 0:
                        has_commit = True
                        lag += hi - tp.offset
                    else:
                        lag += hi - lo
                now = time.time()
                rate = 0.0 if prev_total is None else (total - prev_total) / max(now - prev_t, 1e-9)
                prev_total, prev_t = total, now
                for v in [v for v, t in last_ts.items() if newest - t > a.gone_s]:
                    del last_ts[v]
                n = len(last_ts)
                stale = sum(newest - t > a.stale_s for t in last_ts.values()) / n if n else 0.0
                w.writerow([round(now, 2), total, round(rate, 2), lag if has_commit else -1, n, round(stale, 4)])
                f.flush()
        except KeyboardInterrupt:
            pass
    probe.close()
    meta.close()


if __name__ == "__main__":
    main()
