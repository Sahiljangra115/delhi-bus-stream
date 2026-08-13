"""SYNTHETIC bus network + simulator. Nothing here is real Delhi data.

Used because the real OTD feed needs a private API key (see README). The
simulator emits the same GTFS-realtime snapshots a live poll would, including
GPS noise, stale repeats and dropouts, so the rest of the pipeline is source
agnostic. Ground truth arrival times are kept for validating labels.
"""
from __future__ import annotations

import bisect
import csv
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .geo import Route

SCHED_SPEED = 6.5      # m/s, constant-speed timetable (like the OTD static feed)
SCHED_DWELL = 25.0     # s per stop in the timetable
BASE_SPEED = 8.0       # m/s free-flow cruising speed in the simulator
GPS_SIGMA = 8.0        # m
PING_EVERY = 10        # s, feed refresh period


@dataclass
class Trip:
    trip_id: str
    route_key: str
    sched_start: float
    sched_arr: np.ndarray  # scheduled arrival (s since midnight) per stop


@dataclass
class Network:
    routes: dict
    trips: dict
    headway: dict  # route_key -> scheduled headway s


def _fmt(sec: float) -> str:
    sec = int(round(sec))
    return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


def _parse(s: str) -> float:
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def build_network(n_routes=8, t0=6 * 3600, t1=14 * 3600, seed=0) -> Network:
    rng = np.random.default_rng(seed)
    routes, trips, headway = {}, {}, {}
    for r in range(n_routes):
        lat, lon = 28.50 + rng.random() * 0.25, 77.05 + rng.random() * 0.25
        heading = rng.random() * 2 * math.pi
        n_stops = int(rng.integers(18, 27))
        la, lo = [lat], [lon]
        for _ in range(n_stops - 1):
            heading += rng.normal(0, 0.35)
            d = rng.uniform(450, 850)
            lat += d * math.cos(heading) / 111_320
            lon += d * math.sin(heading) / (111_320 * math.cos(math.radians(lat)))
            la.append(lat)
            lo.append(lon)
        key = f"R{r:02d}_0"
        route = Route(key, [f"R{r:02d}_S{k:02d}" for k in range(n_stops)], la, lo)
        routes[key] = route
        hw = float(rng.integers(4, 8) * 60)
        headway[key] = hw
        t = t0 + float(rng.uniform(0, hw))
        i = 0
        while t < t1:
            arr = t + route.cum / SCHED_SPEED + np.arange(n_stops) * SCHED_DWELL
            tid = f"{key}_T{i:03d}"
            trips[tid] = Trip(tid, key, t, arr)
            t += hw
            i += 1
    return Network(routes, trips, headway)


def write_gtfs(net: Network, out: Path):
    """Write a minimal GTFS static feed (same files a real feed would have)."""
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "stops.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stop_id", "stop_lat", "stop_lon"])
        for r in net.routes.values():
            for s, a, b in zip(r.stop_ids, r.lat, r.lon):
                w.writerow([s, f"{a:.6f}", f"{b:.6f}"])
    with open(out / "routes.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["route_id", "route_short_name"])
        for k in net.routes:
            w.writerow([k.rsplit("_", 1)[0], k.split("_")[0]])
    with open(out / "trips.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["route_id", "service_id", "trip_id", "direction_id"])
        for t in net.trips.values():
            rid, d = t.route_key.rsplit("_", 1)
            w.writerow([rid, "WK", t.trip_id, d])
    with open(out / "stop_times.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trip_id", "arrival_time", "stop_id", "stop_sequence"])
        for t in net.trips.values():
            ids = net.routes[t.route_key].stop_ids
            for k, a in enumerate(t.sched_arr):
                w.writerow([t.trip_id, _fmt(a), ids[k], k])


def _rows(p: Path):
    """CSV rows with stripped keys and values (real feeds have BOMs and padding)."""
    with open(p, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            yield {(k or "").strip(): (v or "").strip() for k, v in r.items()}


def load_gtfs(path: Path) -> Network:
    """Load static GTFS into the Network structure (simulator feed or a real feed).

    One Route per (route_id, direction_id, stop pattern). The first pattern seen
    for a route+direction gets key "<route_id>_<dir>", others get "_p<n>" appended,
    so every trip's schedule has the same length as its route's stop list.
    Real-feed handling: BOM and padded fields, blank arrival_time (falls back to
    departure_time, then linear interpolation between timed stops), trips whose
    stops are missing from stops.txt or with fewer than 2 timed stops are skipped.
    """
    path = Path(path)
    stops = {}
    for r in _rows(path / "stops.txt"):
        try:
            stops[r["stop_id"]] = (float(r["stop_lat"]), float(r["stop_lon"]))
        except (KeyError, ValueError):
            continue
    trip_meta = {}
    for r in _rows(path / "trips.txt"):
        trip_meta[r["trip_id"]] = f"{r['route_id']}_{r.get('direction_id') or 0}"
    st = {}
    for r in _rows(path / "stop_times.txt"):
        tm = r.get("arrival_time") or r.get("departure_time") or ""
        st.setdefault(r["trip_id"], []).append(
            (int(r["stop_sequence"]), r["stop_id"], _parse(tm) if tm else float("nan")))
    routes, trips, patterns = {}, {}, {}
    for tid, rows in st.items():
        if tid not in trip_meta:
            continue
        rows.sort()
        ids = tuple(s for _, s, _ in rows)
        if len(ids) < 2 or any(s not in stops for s in ids):
            continue
        arr = np.array([a for _, _, a in rows], float)
        ok = ~np.isnan(arr)
        if ok.sum() < 2:
            continue
        if not ok.all():
            idx = np.arange(len(arr))
            arr = np.interp(idx, idx[ok], arr[ok])
        base = trip_meta[tid]
        pk = patterns.setdefault(base, {})
        if ids not in pk:
            key = base if not pk else f"{base}_p{len(pk)}"
            pk[ids] = key
            routes[key] = Route(key, list(ids), [stops[s][0] for s in ids], [stops[s][1] for s in ids])
        key = pk[ids]
        trips[tid] = Trip(tid, key, float(arr[0]), arr)
    starts, headway = {}, {}
    for t in trips.values():   # one pass; real feeds have ~10^5 trips
        starts.setdefault(t.route_key, []).append(t.sched_start)
    for key in routes:
        s = sorted(starts.get(key, []))
        headway[key] = float(np.median(np.diff(s))) if len(s) > 1 else 600.0
    return Network(routes, trips, headway)


class _Veh:
    __slots__ = ("trip", "route", "vid", "progress", "k", "dwell", "next_fix", "last_fix",
                 "bus_f", "outage_until", "done", "gone")


class Simulator:
    """1 s physics. run() yields (T, [entity dicts]) every PING_EVERY seconds."""

    def __init__(self, net: Network, t0: float, t1: float, seed: int = 1,
                 dropout=0.03, outage_p=0.002):
        self.net, self.t0, self.t1 = net, t0, t1
        self.rng = np.random.default_rng(seed)
        self.dropout, self.outage_p = dropout, outage_p
        self.truth = {}  # (trip_id, stop_idx) -> true arrival time
        self.last_dep = {}  # (route_key, stop_idx) -> last departure time
        self.cong = {k: np.zeros(len(r.stop_ids) - 1) for k, r in net.routes.items()}

    @staticmethod
    def _tod(t):
        h = t / 3600
        f = 1.0
        for c, w in ((9.0, 0.65), (18.5, 0.7)):  # morning and evening peaks
            f *= 1 - (1 - w) * math.exp(-((h - c) / 1.2) ** 2)
        return f

    def _gps(self, v: _Veh, t):
        r = self.net.routes[v.route]
        la = np.interp(v.progress, r.cum, r.lat)
        lo = np.interp(v.progress, r.cum, r.lon)
        n = self.rng.normal(0, GPS_SIGMA, 2)
        return (la + n[0] / 111_320,
                lo + n[1] / (111_320 * math.cos(math.radians(la))))

    def run(self):
        trips = sorted(self.net.trips.values(), key=lambda x: x.sched_start)
        pending = [(t.sched_start + float(np.clip(self.rng.normal(0, 45), -30, 150)), t)
                   for t in trips]
        pending.sort(key=lambda x: x[0])
        pi, active, vnum = 0, [], 0
        rng = self.rng
        t = float(self.t0)
        while t < self.t1 + 1:
            if int(t) % 60 == 0:
                for k, c in self.cong.items():
                    c[:] = 0.92 * c + rng.normal(0, 0.08, c.shape)
            while pi < len(pending) and pending[pi][0] <= t:
                trip = pending[pi][1]
                v = _Veh()
                v.trip, v.route, v.vid = trip, trip.route_key, f"DL1PC{vnum:04d}"
                v.progress, v.k, v.dwell = 0.0, 1, 0.0
                v.next_fix = t + rng.uniform(0, PING_EVERY)
                v.last_fix, v.bus_f = None, rng.uniform(0.88, 1.1)
                v.outage_until, v.done, v.gone = -1.0, False, False
                self.truth[(trip.trip_id, 0)] = t
                self.last_dep[(v.route, 0)] = t
                active.append(v)
                vnum += 1
                pi += 1
            tod = self._tod(t)
            for v in active:
                if v.done:
                    continue
                r = self.net.routes[v.route]
                if v.dwell > 0:
                    v.dwell -= 1
                    if v.dwell <= 0:
                        self.last_dep[(v.route, v.k - 1)] = t
                else:
                    seg = min(bisect.bisect_right(r.cum, v.progress) - 1, len(r.cum) - 2)
                    sp = BASE_SPEED * tod * math.exp(self.cong[v.route][seg]) * v.bus_f
                    sp = min(max(sp * (1 + 0.08 * rng.standard_normal()), 1.0), 14.0)
                    p_new = v.progress + sp
                    if p_new >= r.cum[v.k]:
                        tc = t + (r.cum[v.k] - v.progress) / sp
                        self.truth[(v.trip.trip_id, v.k)] = tc
                        v.progress = r.cum[v.k]
                        if v.k == len(r.cum) - 1:
                            v.done = True
                        else:
                            prev = self.last_dep.get((v.route, v.k))
                            gap = (tc - prev) if prev is not None else self.net.headway[v.route]
                            v.dwell = float(np.clip(
                                (8 + 0.07 * gap) * math.exp(rng.normal(0, 0.25)), 5, 120))
                            v.k += 1
                    else:
                        v.progress = p_new
                if t >= v.next_fix and t >= v.outage_until:
                    v.next_fix = t + PING_EVERY + rng.uniform(-2, 2)
                    if rng.random() < self.outage_p:
                        v.outage_until = t + rng.uniform(30, 120)
                    elif rng.random() >= self.dropout:
                        la, lo = self._gps(v, t)
                        v.last_fix = (int(t), la, lo)
            t += 1
            if int(t) % PING_EVERY == 0:
                ents = []
                for v in active:
                    if v.last_fix is None or v.gone:
                        continue
                    ts, la, lo = v.last_fix
                    ents.append(dict(vehicle_id=v.vid, trip_id=v.trip.trip_id,
                                     route_id=v.route, lat=la, lon=lo, timestamp=ts))
                    if v.done:
                        v.gone = True
                active = [v for v in active if not v.gone]
                yield int(t), ents
