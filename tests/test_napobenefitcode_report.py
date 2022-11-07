from datetime import date, datetime

import pytest

from dags.workflows.reporting.napo_benefit_code_daily import is_qualified
from dags.workflows.reporting.napo_benefit_code_daily import date_or_empty


@pytest.mark.parametrize(
    "run_date,lockin_period_days,start_date,expected",
    (
        # the run date is the same as the start date so this doesn't qualify
        (date(2022, 10, 1), 3, date(2022, 10, 1), False),
        # Doesn't qualify as the lockin period is 3 days and its only day 2
        (date(2022, 10, 2), 3, date(2022, 10, 1), False),
        # Qualifies as the lockin period is 3 days and its now day 3
        (date(2022, 10, 2), 3, date(2022, 10, 1), False),
    ),
)
def test_is_qualified(run_date, lockin_period_days, start_date, expected):
    assert is_qualified(run_date, lockin_period_days, start_date) == expected


@pytest.mark.parametrize(
    "run_date,expected",
    (
        (datetime(2022, 10, 1, 1, 1, 1), "2022-10-01"),
        (None, ""),
    ),
)
def test_date_or_empty(run_date, expected):
    assert date_or_empty(run_date) == expected
