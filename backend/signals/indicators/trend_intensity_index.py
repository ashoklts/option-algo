from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series
from .sma import calculate_sma

# UNVERIFIED AGAINST A LIVE TRADINGVIEW CHART. Trend Intensity Index (Don
# Worden) is a community indicator with no ta.* built-in. This implements
# the most commonly published structure: deviation from an SMA, summed
# separately for up-deviations and down-deviations over the most recent
# half of the period, expressed as a 0-100 ratio. The specific thing to
# verify is the half-window length - this uses `period // 2` (floor); some
# published versions round instead of flooring.


def calculate_trend_intensity_index(values: Sequence[float], period: int) -> np.ndarray:
    if period < 2:
        raise ValueError("period must be >= 2")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]

    ma = calculate_sma(prices, period)
    diff = prices - ma
    pos = np.where(diff > 0, diff, 0.0)
    neg = np.where(diff < 0, -diff, 0.0)

    half = max(1, period // 2)
    tii = np.full(n, np.nan, dtype=np.float64)

    valid_from = period - 1
    if n - valid_from < half:
        return tii

    pos_windows = np.lib.stride_tricks.sliding_window_view(pos[valid_from:], half)
    neg_windows = np.lib.stride_tricks.sliding_window_view(neg[valid_from:], half)
    pos_sum = pos_windows.sum(axis=-1)
    neg_sum = neg_windows.sum(axis=-1)

    denom = pos_sum + neg_sum
    ratio = np.where(denom == 0, 0.0, 100.0 * pos_sum / denom)

    start = valid_from + half - 1
    tii[start:] = ratio
    return tii


def trend_intensity_index_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> np.ndarray:
    """TII computed directly off raw OHLCV candles - see module docstring for the accuracy caveat."""
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_trend_intensity_index(prices, period)
