import pendulum
import pytest

from dags.workflows.reporting.gocompare.utils import (
    get_monthly_report_name,
    get_weekly_report_name,
)


@pytest.mark.parametrize(
    "run_date,expected_file_name",
    (
        # cross year threshold
        (pendulum.datetime(2022, 12, 26, 0, 0, 0), "napoweek52december2022.csv"),
        (pendulum.datetime(2023, 1, 2, 0, 0, 0), "napoweek01january2023.csv"),
        # cross month threshold
        (pendulum.datetime(2023, 1, 30, 0, 0, 0), "napoweek05january2023.csv"),
        (pendulum.datetime(2023, 2, 6, 0, 0, 0), "napoweek06february2023.csv"),
        # end of 2023
        (pendulum.datetime(2023, 12, 25, 0, 0, 0), "napoweek52december2023.csv"),
        (pendulum.datetime(2024, 1, 1, 0, 0, 0), "napoweek00january2024.csv"),
    ),
)
def test_get_weekly_report_name(run_date: pendulum.datetime, expected_file_name: str):
    assert get_weekly_report_name(run_date) == expected_file_name


@pytest.mark.parametrize(
    "run_date,expected_file_name",
    (
        (pendulum.datetime(2023, 1, 1, 0, 0, 0), "napodecember2022.csv"),
        (pendulum.datetime(2023, 2, 1, 0, 0, 0), "napojanuary2023.csv"),
        (pendulum.datetime(2023, 3, 1, 0, 0, 0), "napofebruary2023.csv"),
    ),
)
def test_get_monthly_report_name(run_date: pendulum.datetime, expected_file_name: str):
    assert get_monthly_report_name(run_date) == expected_file_name
