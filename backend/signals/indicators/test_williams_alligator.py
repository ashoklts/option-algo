from __future__ import annotations

import numpy as np
import pytest

from .williams_alligator import calculate_williams_alligator
from .wilder_smoothing import calculate_wilder_smoothing


def test_lines_are_wilder_smoothed_median_price_shifted_forward():
    rng = np.random.default_rng(6)
    high = 100 + np.cumsum(rng.normal(0, 1, size=60))
    low = high - rng.uniform(1, 3, size=60)
    median_price = (high + low) / 2.0

    result = calculate_williams_alligator(
        high, low, jaw_length=13, jaw_offset=8, teeth_length=8, teeth_offset=5, lips_length=5, lips_offset=3
    )

    jaw_raw = calculate_wilder_smoothing(median_price, 13)
    n = len(high)
    for i in range(8, n):
        if not np.isnan(jaw_raw[i - 8]):
            assert result["jaw"][i] == pytest.approx(jaw_raw[i - 8])
    assert np.all(np.isnan(result["jaw"][:8]))


def test_zero_offset_is_unshifted():
    high = list(range(100, 130))
    low = [h - 2 for h in high]
    result = calculate_williams_alligator(high, low, jaw_length=5, jaw_offset=0, teeth_length=3, teeth_offset=0, lips_length=2, lips_offset=0)
    median_price = (np.asarray(high, dtype=np.float64) + np.asarray(low, dtype=np.float64)) / 2.0
    expected_jaw = calculate_wilder_smoothing(median_price, 5)
    np.testing.assert_allclose(result["jaw"], expected_jaw, equal_nan=True)


def test_default_offsets_match_bill_williams_published_defaults():
    high = list(range(100, 150))
    low = [h - 3 for h in high]
    result = calculate_williams_alligator(high, low)
    # just confirms the call succeeds with defaults and produces the expected shape/NaN warm-up
    assert result["jaw"].shape == (50,)
    assert np.all(np.isnan(result["jaw"][:13 - 1 + 8]))
