from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series
from .wma import calculate_wma


def _pine_round(value: float) -> int:
    """
    Round half away from zero, matching Pine Script's math.round - NOT
    Python's banker's-rounding builtin `round()` (round(4.5) == 4 in Python,
    but TradingView's math.round(4.5) == 5). HMA's internal period/2 and
    sqrt(period) lengths are rounded this way on TradingView.
    """
    return int(math.floor(value + 0.5)) if value >= 0 else int(math.ceil(value - 0.5))


def calculate_hma(values: Sequence[float], period: int) -> np.ndarray:
    """
    Hull Moving Average: WMA(2*WMA(src, period/2) - WMA(src, period), sqrt(period)),
    with period/2 and sqrt(period) each rounded to the nearest int via _pine_round.

    The two WMA calls are where every platform agrees; the rounding rule on a
    non-integer half-length is the one spot platforms sometimes don't (e.g.
    pandas-ta truncates instead of rounding). If a TradingView cross-check
    ever disagrees only on a period whose half lands on X.5, this is the
    line to revisit.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    half_period = _pine_round(period / 2.0)
    sqrt_period = _pine_round(math.sqrt(period))

    prices = np.asarray(values, dtype=np.float64)
    wma_half = calculate_wma(prices, half_period)
    wma_full = calculate_wma(prices, period)
    raw_hull = 2.0 * wma_half - wma_full
    return calculate_wma(raw_hull, sqrt_period)


def hma_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    HMA computed directly off raw OHLCV candles using the indicator's
    `valuePaths` field. Pass `candle_interval` (e.g. "5 minutes", "1 hour")
    when `candles` are 1-minute bars and need aggregating to that interval
    first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_hma(prices, period)
