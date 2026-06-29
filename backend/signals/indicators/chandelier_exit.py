from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .atr import calculate_atr
from .donchian_channel import calculate_donchian_channel


def calculate_chandelier_exit(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    period: int = 22,
    mult: float = 3.0,
) -> dict[str, np.ndarray]:
    """
    Chandelier Exit, matching TradingView's built-in: a long trailing stop at
    highest(high, period) - mult*ATR(period), a short trailing stop at
    lowest(low, period) + mult*ATR(period), each ratcheted so it only ever
    moves in the trade's favour while price stays beyond the opposite stop
    from the prior bar (same "trailing stop never gives back ground" idea as
    SuperTrend's band ratchet). `direction` flips to long when close breaks
    above the short stop, to short when it breaks below the long stop, and
    otherwise holds.
    """
    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    close_arr = np.asarray(close, dtype=np.float64)
    n = high_arr.shape[0]

    atr_band = mult * calculate_atr(high_arr, low_arr, close_arr, period)
    donchian = calculate_donchian_channel(high_arr, low_arr, period)
    long_stop_raw = donchian["upper"] - atr_band
    short_stop_raw = donchian["lower"] + atr_band

    long_stop = np.full(n, np.nan, dtype=np.float64)
    short_stop = np.full(n, np.nan, dtype=np.float64)
    direction = np.full(n, np.nan, dtype=np.float64)

    for i in range(n):
        if np.isnan(long_stop_raw[i]) or np.isnan(short_stop_raw[i]):
            continue

        prev_valid = i > 0 and not np.isnan(long_stop[i - 1])
        long_stop_prev = long_stop[i - 1] if prev_valid else long_stop_raw[i]
        short_stop_prev = short_stop[i - 1] if prev_valid else short_stop_raw[i]

        long_stop[i] = (
            max(long_stop_raw[i], long_stop_prev)
            if prev_valid and close_arr[i - 1] > long_stop_prev
            else long_stop_raw[i]
        )
        short_stop[i] = (
            min(short_stop_raw[i], short_stop_prev)
            if prev_valid and close_arr[i - 1] < short_stop_prev
            else short_stop_raw[i]
        )

        if not prev_valid:
            direction[i] = 1.0
        elif close_arr[i] > short_stop_prev:
            direction[i] = 1.0
        elif close_arr[i] < long_stop_prev:
            direction[i] = -1.0
        else:
            direction[i] = direction[i - 1]

    return {"long_stop": long_stop, "short_stop": short_stop, "direction": direction}


def chandelier_exit_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int = 22,
    mult: float = 3.0,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    Chandelier Exit computed directly off raw OHLCV candles. Pass
    `candle_interval` (e.g. "5 minutes", "1 hour") when `candles` are
    1-minute bars and need aggregating first.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_chandelier_exit(ohlc["high"], ohlc["low"], ohlc["close"], period, mult)
