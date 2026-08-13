"""GTFS-realtime VehiclePositions encode/decode (protobuf)."""
from google.transit import gtfs_realtime_pb2 as pb


def encode(entities, header_ts: int) -> bytes:
    m = pb.FeedMessage()
    m.header.gtfs_realtime_version = "2.0"
    m.header.timestamp = int(header_ts)
    for e in entities:
        fe = m.entity.add()
        fe.id = e["vehicle_id"]
        v = fe.vehicle
        v.trip.trip_id = e["trip_id"]
        v.trip.route_id = e["route_id"].rsplit("_", 1)[0]
        v.vehicle.id = e["vehicle_id"]
        v.position.latitude = e["lat"]
        v.position.longitude = e["lon"]
        v.timestamp = int(e["timestamp"])
    return m.SerializeToString()


def decode(buf: bytes):
    """Return (header_ts, [dict]). route_id is the GTFS route_id (no direction)."""
    m = pb.FeedMessage()
    m.ParseFromString(buf)
    out = []
    for fe in m.entity:
        if not fe.HasField("vehicle"):
            continue
        v = fe.vehicle
        out.append(dict(vehicle_id=v.vehicle.id or fe.id, trip_id=v.trip.trip_id,
                        route_id=v.trip.route_id, lat=v.position.latitude,
                        lon=v.position.longitude, timestamp=int(v.timestamp)))
    return int(m.header.timestamp), out
