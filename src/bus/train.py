"""Train ETA regressor + bunching classifier from the Feast offline store."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import average_precision_score, roc_auc_score

from . import fs
from .features import BUNCH_FRAC, FEATURES
from .sim import load_gtfs


def baselines(df: pd.DataFrame) -> dict:
    """ETA baselines, all in seconds until the bus reaches its next stop."""
    sp_last = df["speed_last"].fillna(df["speed_5m"]).fillna(5.0).clip(lower=1.0)
    sp_5m = df["speed_5m"].fillna(df["speed_last"]).fillna(5.0).clip(lower=1.0)
    return {
        "static_schedule": df["sched_eta_s"],                 # what the timetable says
        "schedule_plus_current_delay": df["sched_eta_s"] + df["delay_s"],
        "last_speed": df["dist_next_m"] / sp_last,            # distance / latest speed
        "speed_5m": df["dist_next_m"] / sp_5m,
    }


def mae_stats(err):
    e = np.abs(np.asarray(err, float))
    return dict(mae=round(float(e.mean()), 2), median=round(float(np.median(e)), 2),
                p90=round(float(np.quantile(e, 0.9)), 2))


def fit_eval(tr, te, truth_te=None):
    reg = HistGradientBoostingRegressor(loss="absolute_error", max_iter=300,
                                        learning_rate=0.08, random_state=0)
    reg.fit(tr[FEATURES], tr["eta_s"])
    pred = reg.predict(te[FEATURES])
    out = {"n_train": len(tr), "n_test": len(te), "target": "label (15 m geofence crossing)",
           "model": mae_stats(pred - te["eta_s"])}
    for k, v in baselines(te).items():
        out[k] = mae_stats(v - te["eta_s"])
    if truth_te is not None:
        out["vs_sim_truth"] = {"model": mae_stats(pred - truth_te)}
        for k, v in baselines(te).items():
            out["vs_sim_truth"][k] = mae_stats(v - truth_te)
    # bunching
    ok_tr, ok_te = tr["bunched"].notna(), te["bunched"].notna()
    clf = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=0)
    clf.fit(tr.loc[ok_tr, FEATURES], tr.loc[ok_tr, "bunched"].astype(int))
    y = te.loc[ok_te, "bunched"].astype(int)
    p = clf.predict_proba(te.loc[ok_te, FEATURES])[:, 1]
    # headway rule: time gap to the bus ahead, as a fraction of the scheduled headway
    ratio = (te.loc[ok_te, "headway_time_s"] / te.loc[ok_te, "sched_headway_s"]).fillna(1e6)
    rule_hit = ratio < BUNCH_FRAC
    rtp = int((rule_hit & (y == 1)).sum())
    thr = 0.5
    tp = int(((p >= thr) & (y == 1)).sum())
    out["bunching"] = dict(n_test=int(len(y)), base_rate=round(float(y.mean()), 3),
                           auc_model=round(float(roc_auc_score(y, p)), 3),
                           auc_headway_rule=round(float(roc_auc_score(y, -ratio)), 3),
                           precision_headway_rule=round(rtp / max(int(rule_hit.sum()), 1), 3),
                           recall_headway_rule=round(rtp / max(int((y == 1).sum()), 1), 3),
                           ap_model=round(float(average_precision_score(y, p)), 3),
                           precision_at_0_5=round(tp / max(int((p >= thr).sum()), 1), 3),
                           recall_at_0_5=round(tp / max(int((y == 1).sum()), 1), 3))
    return reg, clf, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--truth", default="data/truth.csv")
    ap.add_argument("--gtfs", default="data/gtfs")
    a = ap.parse_args()
    d = Path(a.data)
    lab = pd.read_parquet(d / "labels.parquet")
    store = fs.apply()
    ent = lab[["vehicle_id", "event_timestamp"]].copy()
    hist = store.get_historical_features(entity_df=ent, features=fs.refs(FEATURES)).to_df()
    cols = ["vehicle_id", "event_timestamp"]
    lab2 = lab[cols + ["ts", "trip_id", "route_key", "eta_s", "bunched"]]
    df = hist.merge(lab2, on=cols, how="inner")
    df = df.dropna(subset=["eta_s"]).sort_values("event_timestamp").reset_index(drop=True)
    df["sched_headway_s"] = df["route_key"].map(load_gtfs(Path(a.gtfs)).headway)
    res = {}
    if Path(a.truth).exists():
        tr = pd.read_csv(a.truth).set_index(["trip_id", "stop_idx"])["arrival_ts"]
        idx = pd.MultiIndex.from_arrays([df["trip_id"], df["next_stop_idx"].astype(int)])
        df["truth_eta"] = tr.reindex(idx).to_numpy() - df["ts"].to_numpy()
        # label quality: geofence-crossing label vs the simulator's true stop arrival.
        # The label fires 15 m before the stop, so a small negative bias is expected.
        err = (df["eta_s"] - df["truth_eta"]).dropna()
        res["label_vs_sim_truth"] = dict(n=int(len(err)), coverage=round(len(err) / len(df), 4),
                                         bias_s=round(float(err.mean()), 2), **mae_stats(err))
    # primary: hold out 30% of trips (random, seeded)
    rng = np.random.default_rng(0)
    trips = df["trip_id"].unique()
    held = set(rng.choice(trips, int(0.3 * len(trips)), replace=False))
    m = df["trip_id"].isin(held)
    te = df[m]
    reg, clf, res["trip_holdout"] = fit_eval(df[~m], te, te["truth_eta"] if "truth_eta" in df else None)
    joblib.dump({"reg": reg, "clf": clf, "features": FEATURES}, d / "model.joblib")
    # stricter: train on first 70% of time, test on the last 30%
    cut = df["event_timestamp"].quantile(0.7)
    te2 = df[df["event_timestamp"] > cut]
    _, _, res["chronological"] = fit_eval(df[df["event_timestamp"] <= cut], te2,
                                          te2["truth_eta"] if "truth_eta" in df else None)
    (d / "metrics.json").write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
