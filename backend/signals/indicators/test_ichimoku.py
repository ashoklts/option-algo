from __future__ import annotations

import numpy as np
import pytest

from .ichimoku import calculate_ichimoku
from .donchian_channel import calculate_donchian_channel


def test_tenkan_and_kijun_are_donchian_basis():
    rng = np.random.default_rng(4)
    high = 100 + np.cumsum(rng.normal(0, 1, size=80))
    low = high - rng.uniform(1, 3, size=80)
    close = low + rng.uniform(0, 1, size=80) * (high - low)

    result = calculate_ichimoku(high, low, close, conversion_period=9, base_period=26, span_b_period=52, displacement=26)

    expected_tenkan = calculate_donchian_channel(high, low, 9)["basis"]
    expected_kijun = calculate_donchian_channel(high, low, 26)["basis"]
    np.testing.assert_allclose(result["tenkan_sen"], expected_tenkan, equal_nan=True)
    np.testing.assert_allclose(result["kijun_sen"], expected_kijun, equal_nan=True)


def test_senkou_span_a_is_shifted_forward_by_displacement():
    high = list(range(100, 140))
    low = [h - 5 for h in high]
    close = [h - 2 for h in high]

    result = calculate_ichimoku(high, low, close, conversion_period=3, base_period=5, span_b_period=10, displacement=4)

    tenkan = calculate_donchian_channel(high, low, 3)["basis"]
    kijun = calculate_donchian_channel(high, low, 5)["basis"]
    raw_senkou_a = (tenkan + kijun) / 2.0

    # senkou_span_a[i] should equal the *raw* value computed `displacement` bars earlier.
    for i in range(4, len(high)):
        if not np.isnan(raw_senkou_a[i - 4]):
            assert result["senkou_span_a"][i] == pytest.approx(raw_senkou_a[i - 4])
    assert np.all(np.isnan(result["senkou_span_a"][:4]))


def test_chikou_span_pulls_future_close_backward():
    close = list(range(100, 120))
    high = [c + 1 for c in close]
    low = [c - 1 for c in close]

    result = calculate_ichimoku(high, low, close, displacement=5)

    n = len(close)
    for i in range(n - 5):
        assert result["chikou_span"][i] == pytest.approx(close[i + 5])
    assert np.all(np.isnan(result["chikou_span"][n - 5:]))


def test_zero_displacement_is_unshifted():
    high = list(range(100, 120))
    low = [h - 2 for h in high]
    close = [h - 1 for h in high]

    result = calculate_ichimoku(high, low, close, displacement=0)
    tenkan = result["tenkan_sen"]
    kijun = result["kijun_sen"]
    expected = (tenkan + kijun) / 2.0
    np.testing.assert_allclose(result["senkou_span_a"], expected, equal_nan=True)
    np.testing.assert_allclose(result["chikou_span"], close)
