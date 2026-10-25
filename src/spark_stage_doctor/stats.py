"""Small statistics helpers that match what the Spark UI shows."""
from __future__ import annotations

from collections.abc import Sequence


def quantile(values: Sequence[float], q: float) -> float:
    """Nearest-rank quantile, as the Spark UI's summary metrics compute it: sorted[min(int(q * n), n - 1)].
    (numpy's default interpolation gives different medians for small n.)"""
    if not values:
        return 0
    s = sorted(values)
    return s[min(int(q * len(s)), len(s) - 1)]


def median(values: Sequence[float]) -> float:
    return quantile(values, 0.5)


def ratio(a: float, b: float) -> float:
    return a / b if b else float("inf") if a else 0.0


def human_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"


def human_ms(ms: float) -> str:
    if ms < 1000:
        return f"{ms:.0f} ms"
    s = ms / 1000
    if s < 120:
        return f"{s:.1f} s"
    m = s / 60
    return f"{m:.1f} min" if m < 120 else f"{m / 60:.1f} h"
