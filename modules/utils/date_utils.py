from datetime import date
from dateutil.relativedelta import relativedelta


def get_target_yyyymm(months_ago=2):
    """
    Returns the year-month string (yyyy-MM) for the given number of months ago.
    """
    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime("%Y-%m")


def get_month_start_n_months_ago(months_ago: int = 2) -> date:
    """
    Returns the date representing the first day of the month, n months ago.
    """
    return date.today().replace(day=1) - relativedelta(months=months_ago)


def month_window(months_ago: int = 3, today: date = None):
    """
    Returns (start, end) for the month n months ago: start is the first day of that
    month (inclusive), end is the first day of the next month (exclusive).
    ``today`` can be passed in for testing.
    """
    first = (today or date.today()).replace(day=1) - relativedelta(months=months_ago)
    return first, first + relativedelta(months=1)


def resolve_target_month(target_month: str = "", months_ago: int = 3, today: date = None) -> str:
    """
    Returns the 'YYYY-MM' month a pipeline run should process.
    An explicit ``target_month`` (e.g. for a backfill or re-run) wins; if it is blank,
    the month ``months_ago`` months before today is used.
    """
    target_month = (target_month or "").strip()
    if target_month:
        month_window_for(target_month)  # validates the format, raises ValueError if bad
        return target_month
    start, _ = month_window(months_ago, today=today)
    return start.strftime("%Y-%m")


def month_window_for(yyyy_mm: str):
    """
    Returns (start, end) for a 'YYYY-MM' string: first day of that month (inclusive)
    and first day of the next month (exclusive).
    """
    year, month = (int(part) for part in yyyy_mm.split("-"))
    first = date(year, month, 1)
    return first, first + relativedelta(months=1)