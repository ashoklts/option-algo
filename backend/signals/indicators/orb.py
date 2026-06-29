from __future__ import annotations

from typing import Any, Mapping, Sequence

from ._candle_interval import resample_candles

_VALID_SIDES = {"high", "low"}


def calculate_opening_range(day_candles: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    """The first candle bucket of the trading day defines the opening range."""
    first = day_candles[0]
    return {"open": float(first["open"]), "high": float(first["high"]), "low": float(first["low"])}


def is_opening_range_valid(opening_range: Mapping[str, float], max_diff_pct: float) -> bool:
    """
    Filters out abnormally wide opening ranges: only treat the range as
    tradeable if (high-low)/open, as a percentage, is within max_diff_pct -
    matches the `maxDiffBetweenOpenHighAndOpenLowInPercentage` field.
    """
    open_price = opening_range["open"]
    if open_price == 0:
        return False
    range_pct = abs(opening_range["high"] - opening_range["low"]) / open_price * 100.0
    return range_pct <= max_diff_pct


def detect_breakout(day_candles: Sequence[Mapping[str, Any]], opening_range: Mapping[str, float], breakout_side: str) -> int | None:
    """
    First bar (index into day_candles, after the opening-range bar itself)
    whose high/low trades beyond the opening range on `breakout_side`.

    ORB has no single TradingView built-in (`ta.*` has no ORB function) - it's
    a strategy pattern implemented many ways. This uses intrabar high/low
    crossing the level (price *traded* through it) rather than waiting for a
    candle close beyond the level; some platforms require close-confirmation
    instead. If your TradingView/Quantman reference uses close-confirmation,
    swap `bar["high"]`/`bar["low"]` below for `bar["close"]`.
    """
    side = str(breakout_side or "").strip().lower()
    if side not in _VALID_SIDES:
        raise ValueError(f"Unsupported breakoutSide: {breakout_side!r}")

    level = opening_range["high"] if side == "high" else opening_range["low"]
    for i in range(1, len(day_candles)):
        bar = day_candles[i]
        if side == "high" and float(bar["high"]) > level:
            return i
        if side == "low" and float(bar["low"]) < level:
            return i
    return None


def _group_by_day(candles: Sequence[Mapping[str, Any]]) -> list[list[Mapping[str, Any]]]:
    days: list[list[Mapping[str, Any]]] = []
    current_date = None
    for candle in candles:
        date = str(candle["timestamp"])[:10]
        if date != current_date:
            days.append([])
            current_date = date
        days[-1].append(candle)
    return days


def orb_from_candles(
    candles: Sequence[Mapping[str, Any]],
    candle_interval: str,
    max_diff_pct: float,
    breakout_side: str,
) -> list[dict[str, Any]]:
    """
    Opening Range Breakout, one result per trading day: aggregates raw
    1-minute `candles` into `candle_interval` buckets, takes the first bucket
    of each day as the opening range, validates it isn't abnormally wide, and
    reports the first later bar (if any) that breaks out on `breakout_side`.
    """
    resampled = resample_candles(candles, candle_interval)
    results: list[dict[str, Any]] = []

    for day_candles in _group_by_day(resampled):
        opening_range = calculate_opening_range(day_candles)
        valid = is_opening_range_valid(opening_range, max_diff_pct)
        breakout_index = detect_breakout(day_candles, opening_range, breakout_side) if valid else None
        results.append(
            {
                "date": str(day_candles[0]["timestamp"])[:10],
                "opening_range": opening_range,
                "valid": valid,
                "breakout_candle": day_candles[breakout_index] if breakout_index is not None else None,
            }
        )
    return results
