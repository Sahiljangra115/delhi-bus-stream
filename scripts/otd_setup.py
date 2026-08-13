"""Check the OTD live-path prerequisites. Refuses to continue without OTD_API_KEY.

Usage: python scripts/otd_setup.py [path/to/otd_gtfs.zip]
 - verifies OTD_API_KEY is set (never prints it)
 - if a zip is given, extracts it to data/otd_gtfs
 - verifies static GTFS files exist (stops.txt, stop_times.txt, trips.txt, routes.txt)
 - makes ONE request to the realtime feed and reports HTTP status and entity count
"""
import os
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

if not os.environ.get("OTD_API_KEY"):
    sys.exit("OTD_API_KEY not set. See README section 'Live OTD path'. Not falling back to simulated data.")

gt = ROOT / "data" / "otd_gtfs"
if len(sys.argv) > 1:
    gt.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(sys.argv[1]) as z:
        z.extractall(gt)
need = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt"]
missing = [n for n in need if not (gt / n).exists()]
print("static GTFS:", "OK" if not missing else f"MISSING {missing} in {gt}")

import requests
from bus import gtfsrt
from bus.poller import OTD_URL
r = requests.get(OTD_URL, params={"key": os.environ["OTD_API_KEY"]}, timeout=10)
print("realtime HTTP", r.status_code)
if r.ok:
    _, ents = gtfsrt.decode(r.content)
    print("vehicles in snapshot:", len(ents))
sys.exit(1 if missing or not r.ok else 0)
