"""Geometry: haversine, route polyline, progress along route, next-stop logic."""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

R_EARTH = 6_371_008.8  # metres


def haversine(lat1, lon1, lat2, lon2):
    """Great-circle distance in metres. Works on scalars or numpy arrays."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi, dl = p2 - p1, np.radians(lon2) - np.radians(lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R_EARTH * np.arcsin(np.sqrt(a))


@dataclass
class Route:
    """Ordered stops of one route+direction. Path = straight lines between stops."""
    key: str
    stop_ids: list
    lat: np.ndarray
    lon: np.ndarray
    cum: np.ndarray = field(init=False)  # metres along route at each stop

    def __post_init__(self):
        self.lat, self.lon = np.asarray(self.lat, float), np.asarray(self.lon, float)
        seg = haversine(self.lat[:-1], self.lon[:-1], self.lat[1:], self.lon[1:])
        self.cum = np.concatenate([[0.0], np.cumsum(seg)])
        self._lat0 = float(self.lat.mean())
        self._kx = math.cos(math.radians(self._lat0))

    def _xy(self, lat, lon):
        """Local equirectangular metres (accurate enough at city scale)."""
        return (np.radians(lon) * self._kx * R_EARTH, np.radians(lat) * R_EARTH)

    def locate(self, lat: float, lon: float, hint: float | None = None):
        """Project a GPS point on the route.

        Returns (progress_m, next_stop_idx, dist_next_stop_m, off_route_m).
        next stop = first stop strictly ahead of the projected position; at the
        terminal it stays the last stop with distance 0. `hint` (previous
        progress) breaks ties on routes that double back.
        """
        x, y = self._xy(lat, lon)
        sx, sy = self._xy(self.lat, self.lon)
        ax, ay, bx, by = sx[:-1], sy[:-1], sx[1:], sy[1:]
        dx, dy = bx - ax, by - ay
        L2 = np.maximum(dx * dx + dy * dy, 1e-9)
        t = np.clip(((x - ax) * dx + (y - ay) * dy) / L2, 0.0, 1.0)
        d = np.hypot(x - (ax + t * dx), y - (ay + t * dy))
        score = d if hint is None else d + 0.0 * hint
        j = int(np.argmin(score))
        seg_len = self.cum[j + 1] - self.cum[j]
        progress = float(self.cum[j] + t[j] * seg_len)
        nxt = int(np.searchsorted(self.cum, progress, side="right"))
        nxt = min(nxt, len(self.cum) - 1)
        return progress, nxt, float(max(self.cum[nxt] - progress, 0.0)), float(d[j])
