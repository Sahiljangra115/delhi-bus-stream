#!/bin/sh
# Full pipeline on SYNTHETIC data. Raw data goes to data/ (gitignored), results to results/.
# Optional step 5 polls the LIVE OTD feed if OTD_API_KEY is set (load it with: set -a; . ./.env; set +a).
set -e
cd "$(dirname "$0")/.."
. "${ML_VENV:?set ML_VENV to the python venv}/bin/activate"
# CPU only (shared machine): no GPU, few threads
export PYTHONPATH="$PWD/src:$PWD/.pydeps" OMP_NUM_THREADS=4 CUDA_VISIBLE_DEVICES=""
mkdir -p results logs data/live
rm -f data/monitor_live.csv data/monitor_overload.csv
python scripts/make_gtfs.py
STARTED=0
./scripts/kafka.sh status >/dev/null || { ./scripts/kafka.sh start; STARTED=1; }
./scripts/kafka.sh wait
for t in vehicle_positions live overload bench; do ./scripts/kafka.sh reset $t 4; done
for g in history score-live score-overload; do ./scripts/kafka.sh delgroup $g; done

# 1. history: 06:00-14:00 simulated at max speed -> features, labels, model
python -m bus.poller --source sim --truth-out data/truth.csv | tee results/history_produce.json
python -m bus.stream --group history | tee results/stream.json
python -m bus.train | tee logs/train.log
cp data/metrics.json results/metrics.json
python -m bus.bench | tee results/bench.json

# 2. real-time replay at 60x (06:00-07:30), scorer + monitor
python -m bus.monitor --topic live --group score-live --interval 2 --out data/monitor_live.csv &
MON=$!
python -m bus.score --topic live --group score-live --tag live > results/score_live.json &
SC=$!
sleep 15
python -m bus.poller --source sim --topic live --speedup 60 --t0 21600 --t1 27000 | tee results/live_produce.json
wait $SC
kill $MON; wait $MON || true
cat results/score_live.json

# 3. overload: replay 06:00-14:00 at max speed while the same scorer runs
python -m bus.monitor --topic overload --group score-overload --interval 2 --out data/monitor_overload.csv &
MON=$!
python -m bus.score --topic overload --group score-overload --tag overload > results/score_overload.json &
SC=$!
sleep 15
python -m bus.poller --source sim --topic overload | tee results/overload_produce.json
wait $SC
kill $MON; wait $MON || true
cat results/score_overload.json

# 4. parity, monitor CSVs, dashboards
python -m bus.parity | tee results/parity.json
cp data/monitor_live.csv data/monitor_overload.csv results/
python -m bus.dashboard --csv results/monitor_live.csv --out results/dashboard_live.html \
  --title "Bus stream monitor: 60x real-time replay" | tee results/monitor_live_summary.json
python -m bus.dashboard --csv results/monitor_overload.csv --out results/dashboard_overload.html \
  --title "Bus stream monitor: overload replay" | tee results/monitor_overload_summary.json

# 5. optional LIVE OTD feed: publish 180 s to Kafka and measure feed quality. Never fatal.
if [ -n "$OTD_API_KEY" ]; then
  ./scripts/kafka.sh reset otd_live 4
  if python -m bus.poller --source otd --topic otd_live --duration 180 \
       --raw-out data/live/otd_raw.parquet > results/otd_live_produce.json 2> logs/otd_live.log; then
    cat results/otd_live_produce.json
    python scripts/otd_feed_stats.py data/live/otd_raw.parquet | tee results/otd_feed_stats.json || true
  else
    echo "OTD live step failed, see logs/otd_live.log" >&2
  fi
else
  echo "OTD_API_KEY not set: live OTD step skipped"
fi

[ "$STARTED" = 1 ] && ./scripts/kafka.sh stop || true
