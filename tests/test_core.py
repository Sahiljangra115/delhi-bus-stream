import math

import numpy as np
import pytest

from bus import gtfsrt, poller
from bus.features import ARRIVE_R, FeatureEngine
from bus.geo import Route, haversine
from bus.sim import Simulator, build_network, load_gtfs, write_gtfs


def straight_route(n=5, step_m=600.0):
    lat = 28.6 + np.arange(n) * step_m / 111_320
    return Route("R_0", [f"S{i}" for i in range(n)], lat, np.full(n, 77.2))


def test_haversine_one_degree_lat():
    assert haversine(0, 0, 1, 0) == pytest.approx(111_195, rel=1e-3)


def test_locate_projects_and_next_stop():
    r = straight_route()
    lat = 28.6 + 900 / 111_320
    prog, nxt, d, off = r.locate(lat, 77.2)
    assert prog == pytest.approx(900, abs=2)
    assert nxt == 2 and d == pytest.approx(300, abs=2) and off < 1


def test_gtfsrt_roundtrip():
    e = dict(vehicle_id="V1", trip_id="T1", route_id="R_0", lat=28.61, lon=77.21, timestamp=1234)
    ts, out = gtfsrt.decode(gtfsrt.encode([e], 1240))
    assert ts == 1240 and out[0]["vehicle_id"] == "V1" and out[0]["route_id"] == "R"
    assert out[0]["lat"] == pytest.approx(28.61, abs=1e-6) and out[0]["timestamp"] == 1234


def test_gtfs_static_roundtrip(tmp_path):
    net = build_network(2, 6 * 3600, 7 * 3600, seed=3)
    write_gtfs(net, tmp_path)
    net2 = load_gtfs(tmp_path)
    assert set(net2.routes) == set(net.routes) and set(net2.trips) == set(net.trips)
    k = next(iter(net.routes))
    assert net2.routes[k].cum[-1] == pytest.approx(net.routes[k].cum[-1], rel=1e-4)
    t = next(iter(net.trips))
    assert np.allclose(net2.trips[t].sched_arr, net.trips[t].sched_arr, atol=1)


def test_simulator_truth_monotone_and_snapshots():
    net = build_network(2, 6 * 3600, 6.5 * 3600, seed=1)
    sim = Simulator(net, 6 * 3600, 8 * 3600, seed=1)
    n = sum(len(e) for _, e in sim.run())
    assert n > 100
    for tid, trip in net.trips.items():
        ts = [sim.truth[(tid, k)] for k in range(len(trip.sched_arr)) if (tid, k) in sim.truth]
        assert ts == sorted(ts)


def perfect_pings(route, trip, vid, speed=8.0, every=10, t0=0.0, n=40):
    out = []
    for i in range(n):
        p = min(speed * every * i, route.cum[-1])
        out.append(dict(vehicle_id=vid, trip_id=trip, timestamp=t0 + every * i,
                        lat=float(np.interp(p, route.cum, route.lat)),
                        lon=float(np.interp(p, route.cum, route.lon))))
    return out


def make_engine(route):
    from bus.sim import Trip
    trip = Trip("T1", route.key, 0.0, route.cum / 6.5)
    return FeatureEngine({route.key: route}, {"T1": trip}, {route.key: 300.0}), trip


def test_engine_features_and_labels():
    r = straight_route(6)
    eng, _ = make_engine(r)
    labelled = []
    rows = []
    for m in perfect_pings(r, "T1", "V1"):
        row, lab = eng.update(m)
        rows.append(row)
        labelled += lab
    assert rows[0]["since_last_ping_s"] != rows[0]["since_last_ping_s"]  # NaN first ping
    assert rows[5]["speed_5m"] == pytest.approx(8.0, rel=0.02)
    assert rows[5]["since_last_ping_s"] == 10
    # label: bus at 8 m/s reaches stop k geofence at (cum[k]-R)/8 s
    for lab in labelled:
        k = int(lab["next_stop_idx"])
        if k == 5:       # terminal: test pings are clamped at the end of the route
            continue
        assert lab["arrival_ts"] == pytest.approx((r.cum[k] - ARRIVE_R) / 8.0, abs=0.5)
        assert lab["eta_s"] == pytest.approx(lab["arrival_ts"] - lab["ts"])
    assert {int(x["next_stop_idx"]) for x in labelled} >= set(range(1, 5))


def test_engine_drops_duplicates_and_stale():
    r = straight_route()
    eng, _ = make_engine(r)
    ms = perfect_pings(r, "T1", "V1", n=3)
    eng.update(ms[0])
    eng.update(ms[1])
    assert eng.update(ms[1])[0] is None      # same timestamp
    assert eng.update(ms[0])[0] is None      # older
    assert eng.stats["dup"] == 2


def test_engine_headway_between_two_buses():
    r = straight_route(8)
    eng, _ = make_engine(r)
    from bus.sim import Trip
    eng.trips["T2"] = Trip("T2", r.key, 0.0, r.cum / 6.5)
    a = perfect_pings(r, "T1", "A", speed=8.0, n=12)
    b = perfect_pings(r, "T2", "B", speed=8.0, t0=0.0, n=12)
    for x in b:                      # B is the same speed, 400 m behind A
        x["lat"] -= 400 / 111_320
    rows = {}
    for ma, mb in zip(a, b):
        eng.update(ma)
        rows[mb["timestamp"]] = eng.update(mb)[0]
    last = rows[max(rows)]
    assert last["headway_ahead_m"] == pytest.approx(400, abs=15)


def test_otd_without_key_refuses(monkeypatch):
    monkeypatch.delenv("OTD_API_KEY", raising=False)
    with pytest.raises(SystemExit) as e:
        next(poller.otd_snapshots())
    assert "OTD_API_KEY" in str(e.value)


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value, key=None):
        self.sent.append((topic, key, value))

    def poll(self, t):
        pass


def test_publish_dedups_and_labels_source():
    e = dict(vehicle_id="V1", trip_id="T1", route_id="R_0", lat=28.6, lon=77.2, timestamp=100)
    buf = gtfsrt.encode([e], 100)
    p, seen = FakeProducer(), set()
    assert poller.publish(p, "t", buf, "SIMULATED", seen) == 1
    assert poller.publish(p, "t", buf, "SIMULATED", seen) == 0   # stale repeat
    assert b'"source": "SIMULATED"' in p.sent[0][2] and p.sent[0][1] == b"R"


def test_load_gtfs_real_feed_quirks(tmp_path):
    """BOM, padded fields, blank arrival_time, two stop patterns on one route, missing stop."""
    (tmp_path / "stops.txt").write_text(
        "﻿stop_id,stop_name,stop_lat,stop_lon\n"
        "A,a, 28.600000 ,77.2\nB,b,28.605,77.2\nC,c,28.610,77.2\nD,d,28.615,77.2\n", encoding="utf-8")
    (tmp_path / "trips.txt").write_text(
        "route_id,service_id,trip_id\n10,WK,t1\n10,WK,t2\n10,WK,t3\n10,WK,t4\n")
    (tmp_path / "stop_times.txt").write_text(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "t1,06:00:00,06:00:00,A,1\nt1,,,B,2\nt1,06:10:00,06:10:00,C,3\n"
        "t2,,06:20:00,A,1\nt2,06:25:00,06:25:00,B,2\nt2,06:30:00,06:30:00,C,3\n"
        "t3,06:00:00,06:00:00,B,1\nt3,06:05:00,06:05:00,C,2\nt3,24:10:00,24:10:00,D,3\n"
        "t4,06:00:00,06:00:00,A,1\nt4,06:05:00,06:05:00,Z,2\n")
    net = load_gtfs(tmp_path)
    assert set(net.trips) == {"t1", "t2", "t3"}            # t4 uses unknown stop Z
    assert net.trips["t1"].route_key == net.trips["t2"].route_key == "10_0"
    assert net.trips["t3"].route_key == "10_0_p1"
    assert net.trips["t1"].sched_arr[1] == pytest.approx(6 * 3600 + 300)   # interpolated
    assert net.trips["t2"].sched_arr[0] == 6 * 3600 + 1200                 # departure fallback
    assert net.trips["t3"].sched_arr[-1] == 24 * 3600 + 600
    for t in net.trips.values():
        assert len(t.sched_arr) == len(net.routes[t.route_key].stop_ids)
    assert net.headway["10_0"] == 1200
