from __future__ import annotations

from typing import Any, Mapping, Sequence

from ._candle_interval import resample_candles

_VALID_CATEGORIES = {"traditional", "fibonacci", "classic", "demarks", "camarilla", "woodie"}


def calculate_pivot_points(
    high: float,
    low: float,
    close: float,
    category: str,
    open_: float | None = None,
) -> dict[str, float | None]:
    """
    Classic floor-trader pivot levels computed from ONE prior period's
    high/low/close - matches TradingView's "Pivot Points Standard" built-in
    indicator's Traditional/Fibonacci/Classic/Woodie/Camarilla/Demark types.
    Demark also needs the prior period's open; every other type ignores it.
    """
    key = str(category or "").strip().lower()
    if key not in _VALID_CATEGORIES:
        raise ValueError(f"Unsupported pivot category: {category!r}")

    h, l, c = float(high), float(low), float(close)
    rng = h - l

    if key in ("traditional", "classic"):
        p = (h + l + c) / 3.0
        r1, s1 = 2 * p - l, 2 * p - h
        r2, s2 = p + rng, p - rng
        if key == "traditional":
            r3, s3 = p * 2 + (h - 2 * l), p * 2 - (2 * h - l)
        else:
            r3, s3 = p + 2 * rng, p - 2 * rng
        return {"p": p, "r1": r1, "r2": r2, "r3": r3, "s1": s1, "s2": s2, "s3": s3}

    if key == "fibonacci":
        p = (h + l + c) / 3.0
        return {
            "p": p,
            "r1": p + 0.382 * rng,
            "r2": p + 0.618 * rng,
            "r3": p + 1.0 * rng,
            "s1": p - 0.382 * rng,
            "s2": p - 0.618 * rng,
            "s3": p - 1.0 * rng,
        }

    if key == "woodie":
        p = (h + l + 2 * c) / 4.0
        r1, s1 = 2 * p - l, 2 * p - h
        r2, s2 = p + rng, p - rng
        r3, s3 = h + 2 * (p - l), l - 2 * (h - p)
        return {"p": p, "r1": r1, "r2": r2, "r3": r3, "s1": s1, "s2": s2, "s3": s3}

    if key == "camarilla":
        p = (h + l + c) / 3.0
        r1, s1 = c + 1.1 * rng / 12, c - 1.1 * rng / 12
        r2, s2 = c + 1.1 * rng / 6, c - 1.1 * rng / 6
        r3, s3 = c + 1.1 * rng / 4, c - 1.1 * rng / 4
        return {"p": p, "r1": r1, "r2": r2, "r3": r3, "s1": s1, "s2": s2, "s3": s3}

    # demarks
    if open_ is None:
        raise ValueError("Demark pivots need the prior period's open price")
    o = float(open_)
    if c < o:
        x = h + 2 * l + c
    elif c > o:
        x = 2 * h + l + c
    else:
        x = h + l + 2 * c
    p = x / 4.0
    return {"p": p, "r1": x / 2.0 - l, "r2": None, "r3": None, "s1": x / 2.0 - h, "s2": None, "s3": None}


def pivot_points_from_candles(
    candles: Sequence[Mapping[str, Any]],
    category: str,
    time_frame: str = "day",
) -> list[dict[str, Any]]:
    """
    Pivot levels per day/week, each period's levels derived from the PRIOR
    period's OHLC (the standard floor-pivot convention - today trades against
    yesterday's pivot). `time_frame` is "day" or "week", matching the
    indicator's `timeFrame` field. The first period is skipped since it has
    no prior period to derive levels from.
    """
    interval = "1 day" if str(time_frame or "day").strip().lower() == "day" else "1 week"
    periods = resample_candles(candles, interval)

    results: list[dict[str, Any]] = []
    for i in range(1, len(periods)):
        prior = periods[i - 1]
        levels = calculate_pivot_points(prior["high"], prior["low"], prior["close"], category, prior.get("open"))
        results.append({"period_start": periods[i]["timestamp"], **levels})
    return results
