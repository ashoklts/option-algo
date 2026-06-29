from __future__ import annotations

import pandas as pd
import pytest

from ._candle_interval import resample_candles
from .sma import sma_from_candles


def _one_minute_candles(start_ts: str, count: int):
    """Build synthetic 1-minute OHLCV candles, one per minute starting at start_ts."""
    base = pd.Timestamp(start_ts)
    candles = []
    for i in range(count):
        ts = base + pd.Timedelta(minutes=i)
        price = 100 + i
        candles.append(
            {
                "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "open": price,
                "high": price + 0.5,
                "low": price - 0.5,
                "close": price + 0.25,
                "volume": 10,
                "oi": 1000 + i,
            }
        )
    return candles


def test_ten_minute_buckets_anchor_to_market_open_not_midnight():
    candles = _one_minute_candles("2026-01-05T09:15:00", count=20)  # 09:15 .. 09:34
    result = resample_candles(candles, "10 minutes")

    timestamps = [c["timestamp"] for c in result]
    assert timestamps == ["2026-01-05T09:15:00", "2026-01-05T09:25:00"]


def test_ohlcv_aggregation_is_correct_within_a_bucket():
    candles = _one_minute_candles("2026-01-05T09:15:00", count=5)
    result = resample_candles(candles, "5 minutes")

    bucket = result[0]
    assert bucket["open"] == candles[0]["open"]
    assert bucket["close"] == candles[-1]["close"]
    assert bucket["high"] == max(c["high"] for c in candles)
    assert bucket["low"] == min(c["low"] for c in candles)
    assert bucket["volume"] == sum(c["volume"] for c in candles)
    assert bucket["oi"] == candles[-1]["oi"]  # snapshot, not summed


def test_one_day_collapses_full_session_to_one_candle():
    candles = _one_minute_candles("2026-01-05T09:15:00", count=375)  # full 09:15-15:30 session
    result = resample_candles(candles, "1 day")

    assert len(result) == 1
    assert result[0]["open"] == candles[0]["open"]
    assert result[0]["close"] == candles[-1]["close"]


def test_unsupported_interval_raises():
    candles = _one_minute_candles("2026-01-05T09:15:00", count=5)
    with pytest.raises(ValueError):
        resample_candles(candles, "7 minutes")


def test_empty_candles_returns_empty_list():
    assert resample_candles([], "5 minutes") == []


def test_sma_from_candles_resamples_before_averaging():
    candles = _one_minute_candles("2026-01-05T09:15:00", count=20)

    resampled = resample_candles(candles, "10 minutes")
    direct = sma_from_candles(candles, period=2, value_path="Close", candle_interval="10 minutes")
    via_pre_resampled = sma_from_candles(resampled, period=2, value_path="Close")

    assert direct[-1] == pytest.approx(via_pre_resampled[-1])
    assert direct[-1] == pytest.approx((resampled[0]["close"] + resampled[1]["close"]) / 2)
