from __future__ import annotations

from typing import Any, Mapping, Sequence

from ._candle_interval import resample_candles


def calculate_cpr(high: float, low: float, close: float) -> dict[str, float]:
    """
    Central Pivot Range from ONE prior period's high/low/close:
    pivot = (H+L+C)/3, bc (bottom central) = (H+L)/2, tc (top central) = 2*pivot - bc.
    This trio is an unambiguous, universally identical formula.
    """
    h, l, c = float(high), float(low), float(close)
    pivot = (h + l + c) / 3.0
    bc = (h + l) / 2.0
    tc = 2 * pivot - bc
    return {"pivot": pivot, "bc": bc, "tc": tc}


def classify_cpr_width(
    tc: float,
    bc: float,
    reference_price: float,
    narrow_range: float,
    moderately_range: float,
    wide_range: float,
) -> str:
    """
    Classifies CPR width (as a % of reference_price) into narrow/moderate/wide/
    very_wide buckets, used informally for breakout-day prediction in CPR
    trading communities. Unlike pivot/CPR itself, these cutoff *values* are a
    configurable judgment call, not a fixed formula every platform agrees on -
    adjust `narrow_range`/`moderately_range`/`wide_range` to match whatever
    convention you're cross-checking against.
    """
    if reference_price == 0:
        raise ValueError("reference_price must be non-zero")

    width_pct = abs(tc - bc) / reference_price * 100.0
    if width_pct <= narrow_range:
        return "narrow"
    if width_pct <= moderately_range:
        return "moderate"
    if width_pct <= wide_range:
        return "wide"
    return "very_wide"


def cpr_from_candles(candles: Sequence[Mapping[str, Any]], time_frame: str = "day") -> list[dict[str, Any]]:
    """
    CPR per day/week, each period's levels derived from the PRIOR period's
    OHLC (today trades against yesterday's CPR). `time_frame` is "day" or
    "week". The first period is skipped since it has no prior period.
    """
    interval = "1 day" if str(time_frame or "day").strip().lower() == "day" else "1 week"
    periods = resample_candles(candles, interval)

    results: list[dict[str, Any]] = []
    for i in range(1, len(periods)):
        prior = periods[i - 1]
        levels = calculate_cpr(prior["high"], prior["low"], prior["close"])
        results.append({"period_start": periods[i]["timestamp"], **levels})
    return results
