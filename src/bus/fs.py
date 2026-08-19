"""Thin helpers around the Feast repo."""
from pathlib import Path

REPO = Path(__file__).resolve().parents[2] / "feast_repo"


def store():
    from feast import FeatureStore
    return FeatureStore(repo_path=str(REPO))


def apply():
    import sys
    sys.path.insert(0, str(REPO))
    import definitions as d
    (REPO.parent / "data" / "feast").mkdir(parents=True, exist_ok=True)
    s = store()
    s.apply([d.vehicle, d.vehicle_stats])
    return s


def refs(features):
    return [f"vehicle_stats:{f}" for f in features]
