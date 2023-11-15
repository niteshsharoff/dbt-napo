import pendulum
import pytest

from dags.workflows.reporting.msm.utils import (get_monthly_report_name,
                                                get_monthly_reporting_period,
                                                get_weekly_report_name,
                                                get_weekly_reporting_period)


@pytest.mark.parametrize(
    "run_date,expected_file_name",
    (
        # cross year threshold
        (pendulum.datetime(2022, 12, 26, 0, 0, 0), "Napo_Pet_Weekly_20221226.csv"),
        (pendulum.datetime(2023, 1, 2, 0, 0, 0), "Napo_Pet_Weekly_20230102.csv"),
        # cross month threshold
        (pendulum.datetime(2023, 1, 30, 0, 0, 0), "Napo_Pet_Weekly_20230130.csv"),
        (pendulum.datetime(2023, 2, 6, 0, 0, 0), "Napo_Pet_Weekly_20230206.csv"),
        # end of 2023
        (pendulum.datetime(2023, 12, 25, 0, 0, 0), "Napo_Pet_Weekly_20231225.csv"),
        (pendulum.datetime(2024, 1, 1, 0, 0, 0), "Napo_Pet_Weekly_20240101.csv"),
    ),
)
def test_get_weekly_report_name(run_date: pendulum.datetime, expected_file_name: str):
    assert get_weekly_report_name(run_date) == expected_file_name


@pytest.mark.parametrize(
    "run_date,expected_file_name",
    (
        (pendulum.datetime(2023, 1, 1, 0, 0, 0), "Napo_Pet_Monthly_20230101.csv"),
        (pendulum.datetime(2023, 2, 1, 0, 0, 0), "Napo_Pet_Monthly_20230201.csv"),
        (pendulum.datetime(2023, 3, 1, 0, 0, 0), "Napo_Pet_Monthly_20230301.csv"),
    ),
)
def test_get_monthly_report_name(run_date: pendulum.datetime, expected_file_name: str):
    assert get_monthly_report_name(run_date) == expected_file_name


@pytest.mark.parametrize(
    "data_interval_end,expected_start_date,expected_end_date,expected_run_date",
    (
        (
            pendulum.datetime(2023, 5, 15),
            pendulum.datetime(2023, 5, 8),
            pendulum.datetime(2023, 5, 14),
            pendulum.datetime(2023, 5, 15),
        ),
    ),
)
def test_get_weekly_reporting_period(
    data_interval_end: pendulum.datetime,
    expected_start_date: pendulum.datetime,
    expected_end_date: pendulum.datetime,
    expected_run_date: pendulum.datetime,
):
    start_date, end_date, run_date = get_weekly_reporting_period(data_interval_end)
    assert start_date == expected_start_date
    assert end_date == expected_end_date
    assert run_date == expected_run_date


@pytest.mark.parametrize(
    "data_interval_end,expected_start_date,expected_end_date,expected_run_date",
    (
        (
            pendulum.datetime(2023, 6, 1),
            pendulum.datetime(2023, 5, 1),
            pendulum.datetime(2023, 5, 31),
            pendulum.datetime(2023, 6, 1),
        ),
    ),
)
def test_get_monthly_reporting_period(
    data_interval_end: pendulum.datetime,
    expected_start_date: pendulum.datetime,
    expected_end_date: pendulum.datetime,
    expected_run_date: pendulum.datetime,
):
    start_date, end_date, run_date = get_monthly_reporting_period(data_interval_end)
    assert start_date == expected_start_date
    assert end_date == expected_end_date
    assert run_date == expected_run_date
