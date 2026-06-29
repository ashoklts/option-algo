from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .atr import calculate_atr
from .donchian_channel import calculate_donchian_channel
from .sma import calculate_sma

# UNVERIFIED AGAINST A LIVE TRADINGVIEW CHART. "HalfTrend" is a popular
# community script (not a ta.* built-in) and this is a best-effort
# reconstruction from its widely cited structure - confidence here is lower
# than every other indicator in this module. The specific line most likely to
# diverge from your TradingView reference is the trend-transition-bar seed
# (the `up`/`down` assignment right when `trend` flips) - different published
# copies of this script handle that bar slightly differently.
#
# `amplitude` and `channel_deviation` map to this app's `noOfCandles` and
# `multiplier` fields; `atr_period` (default 100) is an internal constant in
# the original script, not exposed in this app's UI.


def calculate_half_trend(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    amplitude: int = 2,
    channel_deviation: float = 2.0,
    atr_period: int = 100,
) -> dict[str, np.ndarray]:
    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    close_arr = np.asarray(close, dtype=np.float64)
    n = high_arr.shape[0]

    dev = channel_deviation * (calculate_atr(high_arr, low_arr, close_arr, atr_period) / 2.0)
    donchian = calculate_donchian_channel(high_arr, low_arr, amplitude)
    high_price = donchian["upper"]
    low_price = donchian["lower"]
    high_ma = calculate_sma(high_arr, amplitude)
    low_ma = calculate_sma(low_arr, amplitude)

    trend = np.zeros(n, dtype=np.int8)
    next_trend = np.zeros(n, dtype=np.int8)
    max_low_price = np.full(n, np.nan, dtype=np.float64)
    min_high_price = np.full(n, np.nan, dtype=np.float64)
    half_trend = np.full(n, np.nan, dtype=np.float64)
    up = np.full(n, np.nan, dtype=np.float64)
    down = np.full(n, np.nan, dtype=np.float64)

    for i in range(n):
        if i == 0:
            max_low_price[i] = low_arr[i]
            min_high_price[i] = high_arr[i]
            continue

        if np.isnan(high_ma[i]) or np.isnan(low_ma[i]) or np.isnan(high_price[i]) or np.isnan(low_price[i]):
            max_low_price[i] = max_low_price[i - 1]
            min_high_price[i] = min_high_price[i - 1]
            trend[i] = trend[i - 1]
            next_trend[i] = next_trend[i - 1]
            continue

        trend[i] = trend[i - 1]
        next_trend[i] = next_trend[i - 1]
        max_low_price[i] = max_low_price[i - 1]
        min_high_price[i] = min_high_price[i - 1]

        if next_trend[i - 1] == 1:
            max_low_price[i] = max(low_price[i], max_low_price[i - 1])
            if high_ma[i] < max_low_price[i] and close_arr[i] < low_arr[i - 1]:
                trend[i] = 1
                next_trend[i] = 0
                min_high_price[i] = high_price[i]
        else:
            min_high_price[i] = min(high_price[i], min_high_price[i - 1])
            if low_ma[i] > min_high_price[i] and close_arr[i] > high_arr[i - 1]:
                trend[i] = 0
                next_trend[i] = 1
                max_low_price[i] = low_price[i]

        if trend[i] == 0:
            if trend[i - 1] != 0:
                up[i] = down[i - 1] if not np.isnan(down[i - 1]) else max_low_price[i]
            else:
                up[i] = max_low_price[i] if np.isnan(up[i - 1]) else max(max_low_price[i], up[i - 1])
            half_trend[i] = up[i]
        else:
            if trend[i - 1] != 1:
                down[i] = up[i - 1] if not np.isnan(up[i - 1]) else min_high_price[i]
            else:
                down[i] = min_high_price[i] if np.isnan(down[i - 1]) else min(min_high_price[i], down[i - 1])
            half_trend[i] = down[i]

    return {"half_trend": half_trend, "trend": trend.astype(np.float64), "dev": dev}


def half_trend_from_candles(
    candles: Sequence[Mapping[str, Any]],
    amplitude: int = 2,
    channel_deviation: float = 2.0,
    atr_period: int = 100,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """Half Trend computed directly off raw OHLCV candles - see module docstring for the accuracy caveat."""
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_half_trend(ohlc["high"], ohlc["low"], ohlc["close"], amplitude, channel_deviation, atr_period)
