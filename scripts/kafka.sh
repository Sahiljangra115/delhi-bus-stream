#!/bin/sh
# Local single-node Kafka in KRaft mode (no ZooKeeper).
# Usage: kafka.sh start|stop|status|wait|topic NAME [PARTITIONS]|reset NAME [PARTITIONS]|delgroup NAME
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
K="$ROOT/.tools/kafka"
LOGDIR="$ROOT/data/kraft-logs"
BS=localhost:9092
# small heap: the machine is shared
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
running() { [ -f "$ROOT/data/kafka.pid" ] && kill -0 "$(cat "$ROOT/data/kafka.pid")" 2>/dev/null; }
case "$1" in
  start)
    if running; then echo "already running"; exit 0; fi
    mkdir -p "$ROOT/data"
    sed "s#^log.dirs=.*#log.dirs=$LOGDIR#" "$K/config/server.properties" > "$ROOT/data/server.properties"
    if [ ! -f "$LOGDIR/meta.properties" ]; then
      ID=$("$K/bin/kafka-storage.sh" random-uuid)
      "$K/bin/kafka-storage.sh" format --standalone -t "$ID" -c "$ROOT/data/server.properties"
    fi
    nohup "$K/bin/kafka-server-start.sh" "$ROOT/data/server.properties" > "$ROOT/data/kafka.out" 2>&1 &
    echo $! > "$ROOT/data/kafka.pid"; echo started ;;
  stop) running && kill "$(cat "$ROOT/data/kafka.pid")" && echo stopped ;;
  status) running && echo running || { echo stopped; exit 1; } ;;
  wait)   # block until the broker answers (max 90 s)
    for _ in $(seq 45); do
      "$K/bin/kafka-topics.sh" --bootstrap-server $BS --list >/dev/null 2>&1 && exit 0
      sleep 2
    done
    echo "broker not ready" >&2; exit 1 ;;
  topic) "$K/bin/kafka-topics.sh" --bootstrap-server $BS --create --if-not-exists \
           --topic "$2" --partitions "${3:-4}" --replication-factor 1 ;;
  reset)   # delete and recreate, so reruns do not append to old data
    "$K/bin/kafka-topics.sh" --bootstrap-server $BS --delete --if-exists --topic "$2"
    for _ in $(seq 20); do
      "$K/bin/kafka-topics.sh" --bootstrap-server $BS --create --if-not-exists \
        --topic "$2" --partitions "${3:-4}" --replication-factor 1 2>/dev/null && exit 0
      sleep 1
    done
    exit 1 ;;
  delgroup)   # drop a consumer group (and its committed offsets) if it exists
    "$K/bin/kafka-consumer-groups.sh" --bootstrap-server $BS --list 2>/dev/null | grep -qx "$2" || exit 0
    "$K/bin/kafka-consumer-groups.sh" --bootstrap-server $BS --delete --group "$2" ;;
esac
