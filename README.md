# Delhi bus arrival prediction (stream pipeline)

Status (2026-10-27): `scripts/run_all.sh` ran end to end (exit 0, `logs/run_all.log`), 11 tests pass (`pytest tests`), every number below comes from a file that run wrote. The model is trained and scored on **SYNTHETIC** data. The live Delhi OTD feed was polled into Kafka and measured, but it is not used for features or training, because the static GTFS (stops, stop_times) for OTD was not downloaded (see "Not done").

Machine for every number: one laptop, Intel i5-12450H (12 threads), 15 GB RAM, CPU only (no GPU), single Kafka broker with a 512 MB heap. **The machine was shared with other heavy jobs during the run** (load average 11.73 on 12 threads at the start, 1.97 at the end, both logged in `logs/run_all.log`). Throughput and latency numbers are therefore lower bounds for this hardware and will move between runs.

## Data honesty

- `src/bus/sim.py` generates a **SYNTHETIC** Delhi-area network (8 routes, random polylines in the Delhi bounding box) and simulates buses with GPS noise (sigma 8 m), stale repeats, dropouts and outages, congestion, and dwell time that grows with the gap to the previous bus (so bunching emerges). It writes standard GTFS static files and emits real GTFS-realtime protobuf snapshots. Every Kafka message from it carries `source: "SIMULATED"`.
- All accuracy figures (ETA MAE, bunching AUC, label error) describe this simulator, not Delhi traffic. The static schedule in the sim is a constant-speed timetable (6.5 m/s, 25 s dwell), tuned once so its median delay is near zero. The "beats the schedule" result depends on that choice.
- The real feed (`https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb`) needs an API key. A key is present in `.env` (gitignored, never printed). Messages from it are tagged `source: "LIVE"`. `bus.poller --source otd` exits with an error when the key is missing and never falls back to simulated data.

## Live OTD feed (measured 2026-10-27)

`scripts/otd_setup.py` with the key (`logs/otd_setup.log`): realtime HTTP 200, 4,988 vehicles in one snapshot; static GTFS reported MISSING (no `data/otd_gtfs`), so the script exits 1. Step 5 of `run_all.sh` then published the live feed to the Kafka topic `otd_live` for 186 s.

| Item | Value | Source |
|---|---|---|
| Published to Kafka (after stale-repeat dedup) | 33,183 msgs in 185.95 s, 178.4 msgs/s | `results/otd_live_produce.json` |
| Polls | 14 (3 corrupt or truncated bodies skipped, 9 with entities) | `results/otd_live_produce.json`, `results/otd_feed_stats.json` |
| Entities per poll (median) | 4,918 | `results/otd_feed_stats.json` |
| Distinct vehicles / routes / trips | 4,997 / 1,337 / 5,089 | `results/otd_feed_stats.json` |
| Rows with trip_id / inside Delhi bounding box | 100% / 99.74% | `results/otd_feed_stats.json` |
| Stale repeats (same vehicle and timestamp in a later poll) | 25.1% of entity rows | `results/otd_feed_stats.json` |
| Per-vehicle update gap p50 / p90 | 22 s / 65 s | `results/otd_feed_stats.json` |
| Feed lag at first sighting (poll time minus vehicle timestamp) p50 / p90 | 28.4 s / 144.3 s | `results/otd_feed_stats.json` |

Caveat: the network was slow during the run (one `ChunkedEncodingError` and three truncated bodies, see `logs/otd_live.log`), so only 14 polls ran in 186 s against a 10 s poll period. The update-gap and lag percentiles above include that client-side slowness; they are an upper bound on what the feed itself delivers. The feed has trip_id and route_id but no stop information, so distance to stop, schedule delay and ETA labels need the static GTFS.

## Architecture

```
sim (or OTD) -> GTFS-RT protobuf -> poller (decode, dedup stale) -> Kafka KRaft (key = route)
  -> FeatureEngine (per-vehicle state) -> parquet (Feast offline) / Feast online (sqlite)
  -> HistGradientBoosting ETA regressor + bunching classifier
  -> bus.monitor (rate, consumer lag, stale share) -> static HTML dashboard
```

- `src/bus/geo.py` haversine, route projection.
- `src/bus/sim.py` network, simulator, GTFS write/load. `load_gtfs` handles real-feed quirks (see "Bugs fixed").
- `src/bus/gtfsrt.py` protobuf encode/decode.
- `src/bus/poller.py` source -> Kafka.
- `src/bus/features.py` rolling 5 min speed, last speed, time since last ping, distance to next stop, headway ahead/behind (distance and time), schedule delay, scheduled ETA, hour. Labels: ETA to next stop (15 m geofence crossing, interpolated between pings) and bunching (arrival gap below 25% of scheduled headway).
- `src/bus/stream.py` drains a topic (named consumer group, `history` in run_all, offsets committed) into `features.parquet`, `labels.parquet`, `raw_pings.parquet`.
- `feast_repo/` one feature view `vehicle_stats`, file offline store, sqlite online store. Training uses `get_historical_features`; scoring writes to and reads from the online store.
- `src/bus/train.py`, `score.py`, `bench.py`, `parity.py`, `monitor.py`, `dashboard.py`.
- `scripts/otd_setup.py` (key and static-GTFS check), `scripts/otd_feed_stats.py` (live feed quality from a `--raw-out` capture).
- Messages are keyed by route so one consumer sees all vehicles of a route in order (headway needs this).

## Deviations from the plan (gaps)

- **No Spark Structured Streaming**: the feature job is a plain Python Kafka consumer. pyspark is not installed in the ml venv. The feature logic needs ordered per-vehicle state and neighbour lookups (headway); with messages keyed by route, one Python consumer handles that directly. Skipped on purpose.
- **Kafka (KRaft, Apache 4.2.2) instead of Redpanda**. Docker is not installed on this machine, so `docker-compose.yml` (Redpanda alternative) was never started.
- **Not deployed through the Smogcast serving stack**; scoring is a standalone consumer script.
- Single machine, single broker, 4 partitions, one consumer per stage.
- Feast and its dependencies (pandas 2.3.3) live in `.pydeps/` (pip `--target`) to leave the shared ml venv untouched. Run with `PYTHONPATH=src:.pydeps`.

## Run

```
set -a; . ./.env; set +a          # optional: enables the live OTD step
ML_VENV=/path/to/venv sh scripts/run_all.sh
```

Needs Java 21 and `.tools/kafka` (extracted from `.tools/kafka_2.13-4.2.2.tgz`). `scripts/kafka.sh` starts a KRaft broker with `-Xmx512m` if none is running and stops it at the end. The full run took about 8.5 min (16:48 to 16:57 on 2026-10-27).

Steps: (1) simulate 06:00 to 14:00 at max speed into `vehicle_positions`, build features and labels, train, Kafka bench; (2) 60x real-time replay of 06:00 to 07:30 into `live` with the scorer (group `score-live`) and the monitor; (3) overload: replay 06:00 to 14:00 at max speed into `overload` while the scorer (group `score-overload`) runs; (4) parity checks and dashboards; (5) live OTD capture if the key is set.

## Monitoring and dashboard

`bus.monitor --topic T --group G` appends every 2 s: wall_ts, msgs_total, msg_rate, consumer lag of group G from its committed offsets (-1 only before G has committed anything), vehicles seen, stale-vehicle fraction (newest ping older than 120 s relative to the stream's newest ping). `bus.score` and `bus.stream` commit offsets after every batch, so the lag is a real number. In this run the only -1 rows are the first 7 samples of each monitor, which fall in the 15 s before the producer starts (topic empty). All later rows are measured values (`results/monitor_live_summary.json`, `results/monitor_overload_summary.json`).

`bus.dashboard` writes static HTML with inline SVG charts and a KPI row (light and dark mode, no server, no JS): `results/dashboard_live.html`, `results/dashboard_overload.html`.

## Results (measured 2026-10-27, run_all.sh, shared machine)

| Item | Value | Source |
|---|---|---|
| Kafka produce / consume msgs per s (bench, 200k msgs of 143 B, lz4, no feature work) | 1,043,598 / 2,146,990 | `results/bench.json` |
| Simulator to Kafka publish rate (protobuf decode + dedup + produce, max speed) | 27,692 msgs/s (158,120 msgs in 5.71 s) | `results/history_produce.json` |
| Feature stage events per s (consume + FeatureEngine, first to last record) | 19,990 events/s (158,120 events in 7.91 s, 0 unknown, 0 off-route) | `results/stream.json` |
| Scoring throughput, real-time replay (60x, about 261 msgs/s offered) | 264.1 msgs/s consumed, 263.5 rows/s scored, keeps up | `results/score_live.json` |
| Consumer lag, real-time replay (monitor, committed offsets) | max 65 msgs, final 0 | `results/monitor_live_summary.json` |
| Scoring throughput under overload (producer at 19,984 msgs/s) | 2,721.6 msgs/s drained, 771.1 rows/s scored | `results/score_overload.json`, `results/overload_produce.json` |
| Backpressure: consumer lag under overload (monitor) | peaked at 132,120 msgs, drained to 0, no messages lost (158,120 consumed of 158,120) | `results/monitor_overload_summary.json`, `results/score_overload.json` |
| End-to-end scoring latency p50 / p95 / p99, real-time replay (publish to prediction) | 63.8 / 94.1 / 167.8 ms (max 248.2) | `results/score_live.json` |
| End-to-end latency under overload p50 / p95 / p99 (queueing included) | 21.4 / 43.3 / 49.0 s | `results/score_overload.json` |
| Mean stage time per batch, real-time (features / Feast write / Feast read / predict) | 2.37 / 19.48 / 3.57 / 6.97 ms | `results/score_live.json` |
| ETA MAE, trip holdout (30% of trips): model / static schedule / schedule + current delay / last speed / 5 min speed | 11.22 / 185.80 / 18.68 / 126.11 / 22.67 s (n_test 46,350) | `results/metrics.json` (`trip_holdout`) |
| ETA MAE, chronological split (last 30% of time) | 11.01 / 162.48 / 21.58 / 138.11 / 21.97 s (n_test 45,536) | `results/metrics.json` (`chronological`) |
| ETA MAE vs simulator ground truth (trip holdout): model / best baseline | 11.33 s / 18.47 s (schedule + current delay) | `results/metrics.json` (`trip_holdout.vs_sim_truth`) |
| Bunching, trip holdout: model AUC / AP / precision / recall at 0.5 | 0.975 / 0.897 / 0.819 / 0.841 (base rate 0.210, n 45,459) | `results/metrics.json` |
| Bunching, headway rule (time gap below 25% of scheduled headway): AUC / precision / recall | 0.859 / 0.817 / 0.800 | `results/metrics.json` |
| Bunching, chronological: model AUC / headway-rule AUC | 0.973 / 0.849 | `results/metrics.json` |
| Label error vs simulator ground truth (geofence label minus true arrival) | MAE 2.67 s, median 2.28 s, p90 4.96 s, bias -0.99 s, coverage 100% (n 151,795) | `results/metrics.json` (`label_vs_sim_truth`) |
| Online vs offline feature parity (Feast online read at scoring time vs Feast offline point-in-time join) | 147 vehicles, all 11 features: 0 NaN mismatches, max abs diff 0.0 | `results/parity.json` |
| Streaming vs independent pandas recompute (5 features, headway not covered) | 158,120 rows, 0 NaN mismatches, max abs diff 0.0 | `results/parity.json` |
| Live OTD feed into Kafka | 178.4 msgs/s, 4,997 vehicles (see "Live OTD feed") | `results/otd_live_produce.json`, `results/otd_feed_stats.json` |

How to read these:

- The Kafka bench builds 200k JSON messages from the 158k history pings (some repeated) and sends them with lz4; it is a ceiling for the broker path, not the pipeline rate.
- Under overload the scorer reads up to 200 messages per batch and scores only the newest row per vehicle in that batch, so `scored` (44,798) is lower than `messages` (158,120). This coalescing is the load-shedding behaviour: every message updates vehicle state, but stale intermediate positions are not scored. Latency under overload is dominated by queueing behind the backlog.
- The scorer's own `max_lag_msgs` field uses cached watermarks (no broker round trip) and is a lower bound; the monitor's committed-offset lag is the number to quote.
- The scoring replay uses the same simulated stream the model was trained on, so it is a systems test, not an accuracy test. Online vs offline parity is exact because the simulator is deterministic (same seed), so the live replay rebuilds the same rows that the history run wrote to the offline store.
- The stale-vehicle share in the overload dashboard (peak 63.6%) is an artefact of replaying 8 hours in 8 s: after the burst, the monitor holds the last pings of buses still on the road at 14:00.

## Bugs found and fixed (2026-10-27)

- **Scorer throttled under overload (the 2026-10-14 run never finished step 3).** `bus.score` sampled lag once a second with `get_watermark_offsets(cached=False)` for each of 4 partitions. Under a backlog each call blocked (profiled at 0.14 s per call; worse earlier), so the scorer processed about one batch every 2 s: the 2026-10-14 monitor CSV (read in this session, then overwritten by the rerun) showed lag falling at exactly 100 msgs/s for 15 min, and `results/score_overload.json` was empty. Fix: cached watermarks. The overload drain rate is now 2,722 msgs/s.
- **`bus.stream` events/s was understated**: it subtracted only the idle wait, so consumer-group join time counted as work. It now times first to last record. It also commits offsets under a named group (`--group history`), so its lag is visible to `bus.monitor`.
- **`load_gtfs` would break on real feeds**: trips of one route and direction with a different stop pattern got the first trip's stop list, so schedule and route lengths disagreed (`np.interp` error or wrong features). It now creates one route per stop pattern. It also handles UTF-8 BOM, padded fields, blank `arrival_time` (falls back to `departure_time`, then interpolates), unknown stop ids, and computes headways in one pass instead of O(routes x trips). Covered by `test_load_gtfs_real_feed_quirks` on a hand-built fixture, **not yet on the real OTD files**.
- `kafka.sh`: heap lowered to 512 MB; `delgroup` command so reruns start each consumer group clean.

## Not done

- **Static OTD GTFS not downloaded, so `load_gtfs` was never run on the real feed and no live features, labels or live accuracy exist.** The download at `https://otd.delhi.gov.in/data/static/` is a POST form that needs a name, an e-mail address and acceptance of the OTD terms and conditions. That is the user's decision, so it was not submitted on their behalf. The mirror linked from the OTD documentation (`traffickarma.iiitd.edu.in:9010`) did not respond. Next step: download the zip by hand, then `python scripts/otd_setup.py path/to/gtfs.zip` and point `bus.stream` / `bus.score` at `--gtfs data/otd_gtfs`.
- **Spark Structured Streaming**: not used (see "Deviations").
- **Redpanda**: not run, Docker is not installed.
- **Smogcast serving stack**: not integrated.
- **Clean-machine throughput**: not measured; all throughput numbers were taken while other jobs used most of the CPU.
