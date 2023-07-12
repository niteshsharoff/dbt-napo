import pendulum


def get_weekly_report_name(run_date: pendulum.datetime):
    return f"napoweek{run_date.strftime('%U%B%Y').lower()}.csv"


def get_monthly_report_name(run_date: pendulum.datetime):
    end_date = run_date.subtract(days=1)
    return f"napo{end_date.strftime('%B%Y').lower()}.csv"
