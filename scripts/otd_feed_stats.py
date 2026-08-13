"""Feed-quality stats for a LIVE OTD capture written by `bus.poller --raw-out`.

Usage: python scripts/otd_feed_stats.py data/live/otd_raw.parquet > results/otd_feed_stats.json
The raw file has one row per entity per poll, before dedup.
"""
import json
import sys

import numpy as np
import pandas as pd

raw = pd.read_parquet(sys.argv[1])
q = lambda s, p: round(float(np.nanquantile(s, p)), 1)
polls = raw.groupby("poll_ts").size()
uniq = raw.drop_duplicates(["vehicle_id", "timestamp"]).sort_values(["vehicle_id", "timestamp"])
gap = uniq.groupby("vehicle_id")["timestamp"].diff().dropna()
lag = raw["poll_ts"] - raw["timestamp"]
fresh = uniq.merge(raw.groupby(["vehicle_id", "timestamp"])["poll_ts"].min().rename("first_poll").reset_index())
first_lag = fresh["first_poll"] - fresh["timestamp"]
in_box = raw["lat"].between(28.3, 28.95) & raw["lon"].between(76.8, 77.4)
print(json.dumps(dict(
    source=str(raw["source"].iloc[0]),
    polls=int(len(polls)), span_s=round(float(raw["poll_ts"].max() - raw["poll_ts"].min()), 1),
    entities_per_poll_median=int(polls.median()),
    entity_rows=int(len(raw)), unique_pings=int(len(uniq)),
    stale_repeat_frac=round(1 - len(uniq) / len(raw), 4),
    vehicles=int(raw["vehicle_id"].nunique()), routes=int(raw["route_id"].replace("", np.nan).nunique()),
    trips=int(raw["trip_id"].replace("", np.nan).nunique()),
    frac_with_trip_id=round(float((raw["trip_id"] != "").mean()), 4),
    frac_in_delhi_bbox=round(float(in_box.mean()), 4),
    update_gap_s=dict(p50=q(gap, .5), p90=q(gap, .9)),
    lag_at_first_sighting_s=dict(p50=q(first_lag, .5), p90=q(first_lag, .9)),
    lag_all_rows_s=dict(p50=q(lag, .5), p90=q(lag, .9)),
), indent=2))
