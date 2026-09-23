"""Unit tests for the RDP accounting math."""
from __future__ import annotations

import math

import pytest

from app import rdp


def test_empty_consumes_nothing():
    curve, eps, _ = rdp.evaluate_entries([], 1e-6)
    assert eps == 0.0
    assert set(curve) == {0.0}


def test_pure_dp_composition_is_exact_addition():
    entries = [(0.1, 0.0), (0.2, 0.0), (0.3, 0.0)]
    _, eps, order = rdp.evaluate_entries(entries, 1e-9)
    assert eps == pytest.approx(0.6)
    assert order == rdp.RDP_ORDERS[0]


def test_single_approx_dp_is_conservative_but_tight_at_high_orders():
    # A single (0.2, 1e-8)-DP mechanism converted at delta=1e-6 must report
    # at least the mechanism's own epsilon, and approach it at high orders.
    _, eps, best_order = rdp.evaluate_entries([(0.2, 1e-8)], 1e-6)
    assert eps >= 0.2 - 1e-12
    assert best_order in (1024.0, 2048.0, 4096.0, 8192.0)


def test_composition_never_underestimates_parts():
    # Basic sanity: the composed guarantee must dominate each constituent
    # mechanism's own (eps, delta)-DP parameters.
    entries = [(0.1, 1e-8), (0.15, 1e-8), (0.05, 1e-7)]
    _, eps, _ = rdp.evaluate_entries(entries, 1e-6)
    for e, _ in entries:
        assert eps >= e


def test_mixed_pure_and_approx():
    # Pure-DP epsilon is added exactly on top of the approximate-DP curve.
    only_approx, eps_approx, _ = rdp.evaluate_entries([(0.1, 1e-8)], 1e-6)
    entries = [(0.3, 0.0), (0.1, 1e-8)]
    _, eps_mixed, _ = rdp.evaluate_entries(entries, 1e-6)
    assert eps_mixed == pytest.approx(eps_approx + 0.3, abs=1e-9)


def test_projection_is_consistent_with_evaluation():
    entries = [(0.1, 1e-8), (0.1, 1e-8)]
    projected, _ = rdp.project_entries(entries, 0.1, 1e-8, 1e-6)
    _, evaluated, _ = rdp.evaluate_entries(
        entries + [(0.1, 1e-8)], 1e-6
    )
    assert projected == pytest.approx(evaluated)


def test_rdp_curve_of_approx_dp():
    curve = rdp.single_rdp_curve(0.2, 1e-8)
    assert len(curve) == len(rdp.RDP_ORDERS)
    # eps_rdp(alpha) = eps + log(1/delta)/(alpha-1), decreasing in alpha.
    assert curve[0] > curve[-1]
    assert curve[0] == pytest.approx(0.2 + math.log(1e8) / (rdp.RDP_ORDERS[0] - 1))


def test_rdp_curve_of_pure_dp_is_flat():
    curve = rdp.single_rdp_curve(0.42, 0.0)
    assert set(curve) == {0.42}
