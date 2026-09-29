"""Re-verify the figures recorded on 2026-09-28 against the live APIs.

This is the project's sourcing standard expressed as tests. Every figure in
constants.py was queried once, by hand, on one day. NYC and NY State open data
update continuously, so these tests exist to answer one question before you
publish: does the live API still say what I wrote down?

A failure here is not necessarily a bug. If the MTA restated ridership, the
test SHOULD fail, and the correct response is to update constants.py and note
the restatement in research/research-log.md — not to loosen the assertion.

These tests hit the network. Run with -m "not network" to skip.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from farecap import collect, constants as C, fareclass  # noqa: E402

pytestmark = pytest.mark.network

# Ridership figures are estimates and get restated. Allow a small relative
# drift before failing, but keep it tight enough to catch a real change.
REL_TOLERANCE = 0.005


def _close(actual: float, expected: float, tol: float = REL_TOLERANCE) -> bool:
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= tol


@pytest.mark.parametrize("year", sorted(C.WINDOWS))
def test_window_total_matches_recorded(year):
    got = collect.window_total(year, refresh=True)["ridership"]
    expected = C.RECORDED_WINDOW_TOTALS[year]
    assert _close(got, expected), (
        f"{year} window ridership drifted: live {got:,.0f} vs recorded {expected:,}. "
        "If the MTA restated, update constants.RECORDED_WINDOW_TOTALS and log it."
    )


@pytest.mark.parametrize("year", sorted(C.WINDOWS))
def test_fare_class_split_reconciles_to_window_total(year):
    """The split must sum to the independently queried total.

    This reconciliation is what caught a silent window-length error during the
    original session: an unbounded '>' filter covered sixteen days, not two,
    and only the total gave it away.
    """
    df = fareclass.composition(year, refresh=True)
    assert df.attrs["reconciles"], (
        f"{year}: fare-class sum {df.attrs['fare_class_sum']:,.0f} != "
        f"window total {df.attrs['window_total']:,.0f}"
    )


@pytest.mark.parametrize("year", sorted(C.WINDOWS))
def test_recorded_fare_classes_still_present_and_close(year):
    live = collect.fare_class_totals(year, refresh=True)
    for category, expected in C.RECORDED_FARE_CLASS_RIDERSHIP[year].items():
        assert category in live, f"{year}: category {category!r} disappeared from schema"
        assert _close(live[category], expected), (
            f"{year} {category}: live {live[category]:,.0f} vs recorded {expected:,}"
        )


def test_seven_day_unlimited_absent_from_2026_schema():
    """The 7-Day Unlimited category is GONE from 2026, not zero.

    If this starts failing, the MTA has reintroduced a 7-day product or
    backfilled the category, and the framing of the piece needs revisiting.
    """
    live = collect.fare_class_totals(2026, refresh=True)
    assert "Metrocard - Unlimited 7-Day" not in live


def test_monthly_pass_is_effectively_extinct_in_2026():
    live = collect.fare_class_totals(2026, refresh=True)
    rides = live.get(C.MONTHLY_PASS_CATEGORY, 0.0)
    total = sum(live.values())
    assert rides / total < 0.0001, (
        "30-Day Unlimited is no longer negligible in 2026 — the premise changed"
    )


def test_ridership_columns_present():
    rows = collect.soql("subway_2025", {"$limit": "1"}, cache=None)
    assert rows, "no rows returned from subway_2025"
    missing = [c for c in C.RIDERSHIP_COLUMNS if c not in rows[0]]
    assert not missing, f"columns missing from 5wq4-mkjj: {missing}"


def test_fair_fares_recent_months_match():
    live = {r["month"]: r["enrollees"] for r in collect.fair_fares(refresh=True)}
    for month, expected in C.RECORDED_FAIR_FARES.items():
        assert month in live, f"Fair Fares month {month} missing"
        assert _close(live[month], expected, tol=0.01), (
            f"Fair Fares {month}: live {live[month]:,} vs recorded {expected:,}"
        )


def test_cap_arithmetic_is_internally_consistent():
    a = fareclass.cap_arithmetic()
    # cap_arithmetic rounds for display, so compare to 2dp not full precision.
    assert a["rides_to_hit_weekly_cap"] == pytest.approx(35.0 / 3.00, abs=0.01)
    assert a["monthly_equivalent_under_weekly_cap"] == pytest.approx(151.67, abs=0.01)
    assert a["eta_proposed_monthly_cap"] == pytest.approx(138.00, abs=0.01)
    assert a["gap_per_month"] > 0
