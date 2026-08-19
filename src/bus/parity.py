"""Feature parity checks.

1. batch: recompute 5 features from the raw ping log with an independent pandas
   path and compare with the streaming engine output (features.parquet).
   Not covered: headway_* features (need cross-vehicle state).
2. feast: for the last scored row of each vehicle in a live run, compare the
   value read from the ONLINE store with the Feast OFFLINE point-in-time join.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import fs
from .features import ARRIVE_R, FEATURES, WINDOW_S
from .sim import load_gtfs


def batch_features(raw: pd.DataFrame, net) -> pd.DataFrame:
    out = []
    for vid, g in raw.sort_values("timestamp").drop_duplicates(["vehicle_id", "timestamp"]).groupby("vehicle_id"):
        trip = net.trips[g["trip_id"].iloc[0]]
        r = net.routes[trip.route_key]
        prog = np.maximum.accumulate(
            [r.locate(a, b)[0] for a, b in zip(g["lat"], g["lon"])])
        ts = g["timestamp"].to_numpy(float)
        nxt = np.minimum(np.searchsorted(r.cum, prog + ARRIVE_R, side="right"), len(r.cum) - 1)
        lo = np.searchsorted(ts, ts - WINDOW_S, side="left")
        span = ts - ts[lo]
        with np.errstate(divide="ignore", invalid="ignore"):
            sp5 = np.where(span >= 10, (prog - prog[lo]) / span, np.nan)
        since = np.concatenate([[np.nan], np.diff(ts)])
        out.append(pd.DataFrame(dict(
            vehicle_id=vid, ts=ts, dist_next_m=r.cum[nxt] - prog, speed_5m=sp5,
            since_last_ping_s=since,
            delay_s=ts - np.interp(prog, r.cum, trip.sched_arr),
            sched_eta_s=trip.sched_arr[nxt] - ts)))
    return pd.concat(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--gtfs", default="data/gtfs")
    ap.add_argument("--live-tag", default="live")
    a = ap.parse_args()
    d = Path(a.data)
    res = {}
    net = load_gtfs(Path(a.gtfs))
    stream = pd.read_parquet(d / "features.parquet")
    stream["ts"] = (stream["event_timestamp"] - pd.Timestamp(0, tz="UTC")).dt.total_seconds()
    bf = batch_features(pd.read_parquet(d / "raw_pings.parquet"), net)
    m = stream.merge(bf, on=["vehicle_id", "ts"], suffixes=("", "_b"))
    res["batch_vs_stream"] = {"rows_compared": len(m), "rows_stream": len(stream)}
    for f in ["dist_next_m", "speed_5m", "since_last_ping_s", "delay_s", "sched_eta_s"]:
        x, y = m[f].to_numpy(float), m[f + "_b"].to_numpy(float)
        same_nan = np.isnan(x) == np.isnan(y)
        diff = np.abs(np.where(np.isnan(x) | np.isnan(y), 0, x - y))
        res["batch_vs_stream"][f] = dict(nan_mismatch=int((~same_nan).sum()),
                                         max_abs_diff=float(diff.max()))
    # online vs offline through Feast
    log = pd.read_parquet(d / f"{a.live_tag}_log.parquet")
    last = log.sort_values("ts").groupby("vehicle_id").tail(1)
    ent = pd.DataFrame(dict(vehicle_id=last["vehicle_id"].to_numpy(),
                            event_timestamp=pd.to_datetime(last["ts"].to_numpy(), unit="s", utc=True)))
    store = fs.apply()
    off = store.get_historical_features(entity_df=ent, features=fs.refs(FEATURES)).to_df()
    off = off.set_index("vehicle_id").loc[last["vehicle_id"]]
    fe = {}
    for f in FEATURES:
        on = last[f"on_{f}"].to_numpy(float)
        of = off[f].to_numpy(float)
        fe[f] = dict(nan_mismatch=int((np.isnan(on) != np.isnan(of)).sum()),
                     max_abs_diff=float(np.abs(np.where(np.isnan(on) | np.isnan(of), 0, on - of)).max()))
    res["online_vs_offline_feast"] = dict(vehicles=len(last), per_feature=fe)
    (d / "parity.json").write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
