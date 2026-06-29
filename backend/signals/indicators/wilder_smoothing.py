from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series
from .sma import calculate_sma


def calculate_wilder_smoothing(values: Sequence[float], period: int) -> np.ndarray:
    """
    Wilder's smoothing (alpha = 1/period), matching TradingView's ta.rma.

    Backs four catalog labels that all reduce to this one formula: "SMMA
    (Smoothed Moving Average)", "Wilder Smoothing Average", "Moving Average
    For ATR", and "Moving Average For RSI" - ATR/RSI just apply this to
    their own derived series (True Range, average gain/loss) instead of
    raw price.

    Seeded with the SMA of the first `period` *valid* bars, then recursed
    from there - unlike ta.ema, which seeds off a single bar. NaN until that
    seed exists.

    `values` may itself start with NaN (e.g. ADX feeds this a DX series that
    is only defined once an upstream RMA has warmed up) - the first non-NaN
    value is treated as bar 0 for warm-up purposes, rather than blindly
    seeding off raw index `period - 1`. Seeding off a NaN would poison the
    entire recursion to NaN forever, since every step depends on the last.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]
    smoothed = np.full(n, np.nan, dtype=np.float64)

    valid_mask = ~np.isnan(prices)
    if not valid_mask.any():
        return smoothed
    first_valid = int(np.argmax(valid_mask))

    if n - first_valid < period:
        return smoothed

    alpha = 1.0 / period
    seed_index = first_valid + period - 1
    smoothed[seed_index] = calculate_sma(prices[first_valid:], period)[period - 1]
    for i in range(seed_index + 1, n):
        smoothed[i] = alpha * prices[i] + (1.0 - alpha) * smoothed[i - 1]
    return smoothed


def wilder_smoothing_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    Wilder smoothing computed directly off raw OHLCV candles using the
    indicator's `valuePaths` field. Pass `candle_interval` (e.g. "5 minutes",
    "1 hour") when `candles` are 1-minute bars and need aggregating to that
    interval first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_wilder_smoothing(prices, period)


# Catalog aliases: same computation, different UI labels / call-sites.
calculate_smma = calculate_wilder_smoothing
smma_from_candles = wilder_smoothing_from_candles
calculate_moving_average_for_atr = calculate_wilder_smoothing
calculate_moving_average_for_rsi = calculate_wilder_smoothing
