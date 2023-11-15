import pendulum
import pytest

from dags.workflows.reporting.cgice.utils import (get_monthly_report_name,
                                                  get_monthly_reporting_period)


@pytest.mark.parametrize(
    "run_date,expected_start_date,expected_end_date",
    (
        (
            pendulum.datetime(2023, 5, 31, 0, 0, 0),
            pendulum.datetime(2023, 5, 1, 0, 0, 0),
            pendulum.datetime(2023, 6, 1, 0, 0, 0),
        ),
        (
            pendulum.datetime(2023, 6, 1, 0, 0, 0),
            pendulum.datetime(2023, 5, 1, 0, 0, 0),
            pendulum.datetime(2023, 6, 1, 0, 0, 0),
        ),
        (
            pendulum.datetime(2023, 6, 2, 0, 0, 0),
            pendulum.datetime(2023, 6, 1, 0, 0, 0),
            pendulum.datetime(2023, 7, 1, 0, 0, 0),
        ),
    ),
)
def test_get_monthly_reporting_period(
    run_date: pendulum.datetime,
    expected_start_date: pendulum.datetime,
    expected_end_date: pendulum.datetime,
):
    assert get_monthly_reporting_period(run_date) == (
        expected_start_date,
        expected_end_date,
    )


@pytest.mark.parametrize(
    "start_date,expected_report_name",
    (
        (
            pendulum.datetime(2023, 5, 31, 0, 0, 0),
            "Napo_Pet_Premium_Bdx_New_2023_05.csv",
        ),
        (
            pendulum.datetime(2023, 6, 1, 0, 0, 0),
            "Napo_Pet_Premium_Bdx_New_2023_06.csv",
        ),
        (
            pendulum.datetime(2023, 6, 2, 0, 0, 0),
            "Napo_Pet_Premium_Bdx_New_2023_06.csv",
        ),
    ),
)
def test_get_monthly_reporting_period(
    start_date: pendulum.datetime,
    expected_report_name: str,
):
    assert get_monthly_report_name(start_date) == expected_report_name
