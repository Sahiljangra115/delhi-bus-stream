"""Write the SYNTHETIC static GTFS used by the simulator into data/gtfs."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bus.sim import build_network, write_gtfs

net = build_network(n_routes=8, t0=6 * 3600, t1=13 * 3600, seed=0)
write_gtfs(net, Path(__file__).resolve().parents[1] / "data" / "gtfs")
print(f"routes={len(net.routes)} trips={len(net.trips)}")
