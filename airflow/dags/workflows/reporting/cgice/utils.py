from typing import Tuple

import pendulum


def get_monthly_reporting_period(
    run_date: pendulum.datetime,
) -> Tuple[pendulum.datetime, pendulum.datetime]:
    # Check if the run date is the first day of the month
    if run_date.day == 1:
        start_date = run_date.subtract(months=1).start_of("month")
        end_date = run_date.start_of("day")
    else:
        start_date = run_date.start_of("month")
        end_date = run_date.end_of("month").add(days=1).start_of("day")

    return start_date, end_date


def get_monthly_report_name(start_date: pendulum.datetime) -> str:
    # Original report name is Napo_Pet_Premium_Bdx_New_{year}_{month}
    return f"Napo_Pet_Premium_Bdx_New_{start_date.format('YYYY_MM')}.csv"
