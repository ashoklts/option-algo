from __future__ import annotations

import pytest

from .cpr import calculate_cpr, classify_cpr_width


def test_normal_day_tc_above_bc():
    # close above the day's H/L midpoint -> standard CPR (tc > bc)
    result = calculate_cpr(high=110, low=90, close=105)
    assert result["pivot"] == pytest.approx((110 + 90 + 105) / 3)
    assert result["bc"] == pytest.approx((110 + 90) / 2)
    assert result["tc"] == pytest.approx(2 * result["pivot"] - result["bc"])
    assert result["tc"] > result["bc"]


def test_inverted_day_tc_below_bc():
    """
    When close sits below the H/L midpoint, tc < bc - this is the
    well-known "inverted CPR" case CPR traders watch for, NOT a bug to be
    swapped/corrected. Swapping max/min here would destroy a real signal.
    """
    result = calculate_cpr(high=112, low=90, close=98)
    assert result["pivot"] == pytest.approx(100)
    assert result["bc"] == pytest.approx(101)
    assert result["tc"] == pytest.approx(99)
    assert result["tc"] < result["bc"]


def test_classify_cpr_width_buckets():
    assert classify_cpr_width(tc=100.2, bc=100.0, reference_price=100, narrow_range=0.3, moderately_range=0.6, wide_range=1.2) == "narrow"
    assert classify_cpr_width(tc=100.5, bc=100.0, reference_price=100, narrow_range=0.3, moderately_range=0.6, wide_range=1.2) == "moderate"
    assert classify_cpr_width(tc=101.0, bc=100.0, reference_price=100, narrow_range=0.3, moderately_range=0.6, wide_range=1.2) == "wide"
    assert classify_cpr_width(tc=102.0, bc=100.0, reference_price=100, narrow_range=0.3, moderately_range=0.6, wide_range=1.2) == "very_wide"
