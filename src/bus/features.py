"""Streaming feature + label engine. One instance handles every route it is fed.

Kafka messages are keyed by route, so one consumer sees all vehicles of a route
in order, which is what the headway features need.
"""
from __future__ import annotations

import math
from collections import deque

import numpy as np

ARRIVE_R = 15.0        # m: a stop counts as reached inside this radius
WINDOW_S = 300         # rolling speed window
LEADER_MAX_AGE = 120   # s: ignore neighbours whose last ping is older
BUNCH_FRAC = 0.25      # bunched if arrival gap < 25% of scheduled headway
FEATURES = ["dist_next_m", "speed_5m", "speed_last", "since_last_ping_s",
            "headway_ahead_m", "headway_behind_m", "headway_time_s",
            "delay_s", "sched_eta_s", "next_stop_idx", "hour"]
NAN = float("nan")


class _V:
    def __init__(self):
        self.hist = deque()   # (ts, progress) within WINDOW_S
        self.last_ts = None
        self.progress = 0.0   # monotone
        self.pending = []     # unlabelled rows
        self.route = None


class FeatureEngine:
    def __init__(self, routes: dict, trips: dict, headway: dict):
        self.routes, self.trips, self.headway = routes, trips, headway
        self.v: dict[str, _V] = {}
        self.by_route: dict[str, dict] = {k: {} for k in routes}  # vid -> (ts, progress)
        self.last_arr: dict = {}
        self.stats = dict(seen=0, dup=0, unknown=0, off_route=0)

    def update(self, m: dict):
        """Process one ping. Returns (feature_row | None, [finalised labelled rows])."""
        self.stats["seen"] += 1
        trip = self.trips.get(m["trip_id"])
        if trip is None:
            self.stats["unknown"] += 1
            return None, []
        route = self.routes[trip.route_key]
        vid, ts = m["vehicle_id"], float(m["timestamp"])
        st = self.v.get(vid)
        if st is None:
            st = self.v[vid] = _V()
            st.route = trip.route_key
        if st.last_ts is not None and ts <= st.last_ts:
            self.stats["dup"] += 1
            return None, []
        prog_raw, _, _, off = route.locate(m["lat"], m["lon"])
        if off > 300:
            self.stats["off_route"] += 1
            return None, []
        prev_prog, prev_ts = st.progress, st.last_ts
        prog = max(prog_raw, prev_prog)
        cum = route.cum
        labelled = self._close_labels(st, route, trip, prev_ts, prev_prog, ts, prog)

        nxt = min(int(np.searchsorted(cum, prog + ARRIVE_R, side="right")), len(cum) - 1)
        dist = float(cum[nxt] - prog)
        st.hist.append((ts, prog))
        while st.hist and st.hist[0][0] < ts - WINDOW_S:
            st.hist.popleft()
        t0, p0 = st.hist[0]
        speed_5m = (prog - p0) / (ts - t0) if ts - t0 >= 10 else NAN
        speed_last = (prog - prev_prog) / (ts - prev_ts) if prev_ts is not None else NAN
        since = ts - prev_ts if prev_ts is not None else NAN

        peers = self.by_route[trip.route_key]
        ahead = behind = None
        for ovid, (ots, op) in peers.items():
            if ovid == vid or ts - ots > LEADER_MAX_AGE:
                continue
            d = op - prog
            if d > 0 and (ahead is None or d < ahead):
                ahead = d
            if d < 0 and (behind is None or -d < behind):
                behind = -d
        peers[vid] = (ts, prog)
        h_t = ahead / max(speed_5m, 1.0) if ahead is not None and speed_5m == speed_5m else NAN
        sched_here = float(np.interp(prog, cum, trip.sched_arr))
        row = dict(vehicle_id=vid, route_key=trip.route_key, trip_id=trip.trip_id, ts=ts,
                   dist_next_m=dist, speed_5m=speed_5m, speed_last=speed_last,
                   since_last_ping_s=since,
                   headway_ahead_m=ahead if ahead is not None else NAN,
                   headway_behind_m=behind if behind is not None else NAN,
                   headway_time_s=h_t, delay_s=ts - sched_here,
                   sched_eta_s=float(trip.sched_arr[nxt] - ts),
                   next_stop_idx=float(nxt),
                   hour=(ts % 86400) / 3600.0,
                   _progress=prog)
        st.pending.append(row)
        st.progress, st.last_ts = prog, ts
        return row, labelled

    def _close_labels(self, st, route, trip, ts0, p0, ts1, p1):
        out = []
        if ts0 is None or p1 <= p0 or not st.pending:
            return out
        cum = route.cum
        crossed = {}
        for k in range(len(cum)):
            thr = cum[k] - ARRIVE_R
            if p0 < thr <= p1:
                crossed[k] = ts0 + (thr - p0) / (p1 - p0) * (ts1 - ts0)
        if not crossed:
            return out
        keep = []
        for row in st.pending:
            k = int(row["next_stop_idx"])
            if k in crossed:
                row = dict(row)
                row["eta_s"] = crossed[k] - row["ts"]
                row["arrival_ts"] = crossed[k]
                out.append(row)
            else:
                keep.append(row)
        st.pending = keep
        for k, tc in crossed.items():
            key = (st.route, k)
            prev = self.last_arr.get(key)
            hw = self.headway.get(st.route, 300.0)
            b = NAN if prev is None else float((tc - prev) < BUNCH_FRAC * hw)
            self.last_arr[key] = tc
            for row in out:
                if int(row["next_stop_idx"]) == k and "bunched" not in row:
                    row["bunched"] = b
        return out
