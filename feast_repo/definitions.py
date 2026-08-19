"""Single definition of the vehicle features, used for training (offline,
point-in-time join) and for scoring (online store)."""
from datetime import timedelta

from feast import Entity, ValueType, FeatureView, Field, FileSource
from feast.types import Float64

vehicle = Entity(name="vehicle", join_keys=["vehicle_id"], value_type=ValueType.STRING)

# Offline store: parquet written by the stream job (one row per accepted ping).
source = FileSource(path="../data/features.parquet", timestamp_field="event_timestamp")

FEATURES = ["dist_next_m", "speed_5m", "speed_last", "since_last_ping_s",
            "headway_ahead_m", "headway_behind_m", "headway_time_s",
            "delay_s", "sched_eta_s", "next_stop_idx", "hour"]

vehicle_stats = FeatureView(
    name="vehicle_stats",
    entities=[vehicle],
    ttl=timedelta(0),   # simulated timestamps; no expiry
    schema=[Field(name=f, dtype=Float64) for f in FEATURES],
    source=source,
)
