from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .donchian_channel import calculate_donchian_channel


def calculate_ichimoku(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    conversion_period: int = 9,
    base_period: int = 26,
    span_b_period: int = 52,
    displacement: int = 26,
) -> dict[str, np.ndarray]:
    """
    Ichimoku Cloud. Tenkan-sen/Kijun-sen/the Senkou Span B base are each just
    a Donchian Channel basis at a different period - (highest+lowest)/2 -
    so this reuses donchian_channel.py rather than re-deriving that midpoint.

    Pine plots Senkou A/B `displacement` bars *ahead* and Chikou `displacement`
    bars *behind* using a chart-only offset, not by shifting the underlying
    series. For signal evaluation you usually want the values actually active
    *at* the current bar, so this function returns the shifted/aligned
    series instead: senkou_span_a[i]/senkou_span_b[i] is the cloud boundary
    that was computed `displacement` bars ago and is overlaying bar i right
    now; chikou_span[i] is today's close as it appears `displacement` bars
    in the past. All three are NaN until enough history/future exists.
    """
    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    close_arr = np.asarray(close, dtype=np.float64)
    n = high_arr.shape[0]

    tenkan = calculate_donchian_channel(high_arr, low_arr, conversion_period)["basis"]
    kijun = calculate_donchian_channel(high_arr, low_arr, base_period)["basis"]
    senkou_b_raw = calculate_donchian_channel(high_arr, low_arr, span_b_period)["basis"]
    senkou_a_raw = (tenkan + kijun) / 2.0

    senkou_a = np.full(n, np.nan, dtype=np.float64)
    senkou_b = np.full(n, np.nan, dtype=np.float64)
    chikou = np.full(n, np.nan, dtype=np.float64)

    if displacement == 0:
        senkou_a, senkou_b, chikou = senkou_a_raw, senkou_b_raw, close_arr.copy()
    elif 0 < displacement < n:
        senkou_a[displacement:] = senkou_a_raw[: n - displacement]
        senkou_b[displacement:] = senkou_b_raw[: n - displacement]
        chikou[: n - displacement] = close_arr[displacement:]

    return {
        "tenkan_sen": tenkan,
        "kijun_sen": kijun,
        "senkou_span_a": senkou_a,
        "senkou_span_b": senkou_b,
        "chikou_span": chikou,
    }


def ichimoku_from_candles(
    candles: Sequence[Mapping[str, Any]],
    conversion_period: int = 9,
    base_period: int = 26,
    span_b_period: int = 52,
    displacement: int = 26,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    Ichimoku Cloud computed directly off raw OHLCV candles. Pass
    `candle_interval` (e.g. "5 minutes", "1 hour") when `candles` are
    1-minute bars and need aggregating first.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_ichimoku(
        ohlc["high"], ohlc["low"], ohlc["close"], conversion_period, base_period, span_b_period, displacement
    )
