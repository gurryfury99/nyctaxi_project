from datetime import date

import pytest

from modules.utils.date_utils import month_window, month_window_for, resolve_target_month


def test_resolve_target_month_prefers_explicit_value():
    assert resolve_target_month("2026-03", months_ago=3, today=date(2026, 8, 15)) == "2026-03"


def test_resolve_target_month_blank_uses_offset():
    assert resolve_target_month("", months_ago=3, today=date(2026, 8, 15)) == "2026-05"
    assert resolve_target_month("  ", months_ago=3, today=date(2026, 8, 15)) == "2026-05"


def test_resolve_target_month_rejects_bad_format():
    with pytest.raises(ValueError):
        resolve_target_month("May 2026")


def test_month_window_three_months_ago():
    assert month_window(3, today=date(2026, 8, 15)) == (date(2026, 5, 1), date(2026, 6, 1))


def test_month_window_crosses_year_boundary():
    assert month_window(3, today=date(2026, 2, 10)) == (date(2025, 11, 1), date(2025, 12, 1))


def test_month_window_end_is_exclusive_first_of_next_month():
    start, end = month_window(1, today=date(2026, 3, 31))
    assert (start, end) == (date(2026, 2, 1), date(2026, 3, 1))


def test_month_window_for_string():
    assert month_window_for("2026-05") == (date(2026, 5, 1), date(2026, 6, 1))
    assert month_window_for("2025-12") == (date(2025, 12, 1), date(2026, 1, 1))
