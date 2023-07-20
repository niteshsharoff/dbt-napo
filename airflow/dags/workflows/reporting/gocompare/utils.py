from typing import Tuple

import pendulum


def get_weekly_reporting_period(
    run_date: pendulum.datetime,
) -> Tuple[pendulum.datetime, pendulum.datetime, pendulum.datetime]:
    end_date = run_date.subtract(days=1)
    start_date = pendulum.parse(f"{end_date.year}W{end_date.week_of_year:02d}")
    return start_date, end_date, run_date


def get_monthly_reporting_period(
    run_date: pendulum.datetime,
) -> Tuple[pendulum.datetime, pendulum.datetime, pendulum.datetime]:
    end_date = run_date.subtract(days=1)
    start_date = run_date.subtract(months=1)
    return start_date, end_date, run_date


def get_weekly_report_name(run_date: pendulum.datetime):
    return f"napoweek{run_date.strftime('%U%B%Y').lower()}.csv"


def get_monthly_report_name(run_date: pendulum.datetime):
    end_date = run_date.subtract(days=1)
    return f"napo{end_date.strftime('%B%Y').lower()}.csv"


def get_weekly_view_name(start_date: pendulum.datetime):
    return "gocompare_sales_report_weekly_{}".format(start_date.format("YYYYMMDD"))


def get_monthly_view_name(start_date: pendulum.datetime):
    return "gocompare_sales_report_monthly_{}".format(start_date.format("YYYYMMDD"))
