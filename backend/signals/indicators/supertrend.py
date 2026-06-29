from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .atr import calculate_atr


def calculate_supertrend(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    period: int,
    multiplier: float,
) -> dict[str, np.ndarray]:
    """
    SuperTrend, matching TradingView's ta.supertrend(factor, atrPeriod):
    bands are built off hl2 +/- multiplier*ATR, then ratcheted so they only
    ever move in the trend's favour (an upper band never rises while in an
    uptrend's prior bar, a lower band never falls while in a downtrend's
    prior bar) - that ratchet is what makes this a trailing stop rather than
    a plain band. Direction flips only when price closes through the band
    on the opposite side. direction=-1 is uptrend (supertrend=lowerBand),
    direction=1 is downtrend (supertrend=upperBand) - same sign convention
    Pine uses.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    close_arr = np.asarray(close, dtype=np.float64)
    n = high_arr.shape[0]

    src = (high_arr + low_arr) / 2.0
    atr = calculate_atr(high_arr, low_arr, close_arr, period)

    upper_band = src + multiplier * atr
    lower_band = src - multiplier * atr

    final_upper = np.full(n, np.nan, dtype=np.float64)
    final_lower = np.full(n, np.nan, dtype=np.float64)
    supertrend = np.full(n, np.nan, dtype=np.float64)
    direction = np.full(n, np.nan, dtype=np.float64)

    for i in range(n):
        if np.isnan(atr[i]):
            continue

        prev_atr_is_nan = i == 0 or np.isnan(atr[i - 1])
        if prev_atr_is_nan:
            final_upper[i] = upper_band[i]
            final_lower[i] = lower_band[i]
            direction[i] = 1.0
        else:
            final_lower[i] = (
                lower_band[i]
                if (lower_band[i] > final_lower[i - 1] or close_arr[i - 1] < final_lower[i - 1])
                else final_lower[i - 1]
            )
            final_upper[i] = (
                upper_band[i]
                if (upper_band[i] < final_upper[i - 1] or close_arr[i - 1] > final_upper[i - 1])
                else final_upper[i - 1]
            )
            if supertrend[i - 1] == final_upper[i - 1]:
                direction[i] = -1.0 if close_arr[i] > final_upper[i] else 1.0
            else:
                direction[i] = 1.0 if close_arr[i] < final_lower[i] else -1.0

        supertrend[i] = final_lower[i] if direction[i] == -1.0 else final_upper[i]

    return {"supertrend": supertrend, "direction": direction}


def supertrend_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    multiplier: float,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    SuperTrend computed directly off raw OHLCV candles. Pass `candle_interval`
    (e.g. "5 minutes", "1 hour") when `candles` are 1-minute bars and need
    aggregating first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_supertrend(ohlc["high"], ohlc["low"], ohlc["close"], period, multiplier)
