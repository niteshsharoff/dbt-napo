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
