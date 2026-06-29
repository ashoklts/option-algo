from __future__ import annotations

import pytest

from .pivot_points import calculate_pivot_points

H, L, C, O = 110.0, 90.0, 100.0, 95.0  # close > open -> Demark's "c > o" branch


def test_traditional():
    r = calculate_pivot_points(H, L, C, "traditional")
    assert r["p"] == pytest.approx(100)
    assert r["r1"] == pytest.approx(110)
    assert r["s1"] == pytest.approx(90)
    assert r["r2"] == pytest.approx(120)
    assert r["s2"] == pytest.approx(80)
    assert r["r3"] == pytest.approx(130)
    assert r["s3"] == pytest.approx(70)


def test_classic():
    r = calculate_pivot_points(H, L, C, "classic")
    assert r["p"] == pytest.approx(100)
    assert r["r1"] == pytest.approx(110)
    assert r["s1"] == pytest.approx(90)
    assert r["r3"] == pytest.approx(140)
    assert r["s3"] == pytest.approx(60)


def test_fibonacci():
    r = calculate_pivot_points(H, L, C, "fibonacci")
    assert r["p"] == pytest.approx(100)
    assert r["r1"] == pytest.approx(107.64)
    assert r["s1"] == pytest.approx(92.36)
    assert r["r2"] == pytest.approx(112.36)
    assert r["s2"] == pytest.approx(87.64)
    assert r["r3"] == pytest.approx(120)
    assert r["s3"] == pytest.approx(80)


def test_woodie():
    r = calculate_pivot_points(H, L, C, "woodie")
    assert r["p"] == pytest.approx(100)
    assert r["r1"] == pytest.approx(110)
    assert r["s1"] == pytest.approx(90)
    assert r["r3"] == pytest.approx(130)
    assert r["s3"] == pytest.approx(70)


def test_camarilla():
    r = calculate_pivot_points(H, L, C, "camarilla")
    assert r["p"] == pytest.approx(100)
    assert r["r1"] == pytest.approx(100 + 1.1 * 20 / 12)
    assert r["s1"] == pytest.approx(100 - 1.1 * 20 / 12)
    assert r["r3"] == pytest.approx(105.5)
    assert r["s3"] == pytest.approx(94.5)


def test_demark_close_above_open():
    r = calculate_pivot_points(H, L, C, "demarks", open_=O)
    assert r["p"] == pytest.approx(102.5)
    assert r["r1"] == pytest.approx(115)
    assert r["s1"] == pytest.approx(95)
    assert r["r2"] is None
    assert r["s2"] is None


def test_demark_requires_open():
    with pytest.raises(ValueError):
        calculate_pivot_points(H, L, C, "demarks")


def test_unsupported_category_raises():
    with pytest.raises(ValueError):
        calculate_pivot_points(H, L, C, "not_a_category")
