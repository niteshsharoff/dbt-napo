import csv
import logging
import argparse
from datetime import datetime, date
from datetime import timezone
from datetime import timedelta
from typing import Optional

import pandas as pd
from dateutil.parser import parse

from .utils import load_csv_to_gcs_bucket
from .utils import filter_by_version_id
from .utils import read_raw_table_by_date


def get_policies(project_id) -> pd.DataFrame:
    """Recover all policies which we'll filter."""
    return read_raw_table_by_date(
        project_id=project_id,
        table_name="policy",
        columns=[
            "version_id",
            "reference_number",
            "quote_id",
            "annual_payment_id",
            "annual_price",
            "payment_plan_type",
            "subscription_id",
            "policy_id",
            "customer_id",
            "cancel_date",
            "created_date",
            "start_date",
            "end_date",
        ],
        date_column=None,
    )


def get_napobenefitcode(project_id: str, run_date: str) -> pd.DataFrame:
    """Recover the latest list of Napo Benefit Codes."""
    start_date = run_date
    end_date = parse(start_date) + timedelta(days=1)
    end_date = end_date.date().isoformat()
    return read_raw_table_by_date(
        project_id=project_id,
        table_name="napobenefitcode",
        columns=[
            "id",
            "code",
        ],
        date_column="run_date",
        start_date=start_date,
        end_date=end_date,
    )


def get_quotewithbenefit(project_id: str) -> pd.DataFrame:
    """Recover all the quotes with benefits which we will join on the policies."""
    return read_raw_table_by_date(
        project_id=project_id,
        table_name="quotewithbenefit",
        columns=[
            "id",
            "quote_id",
            "benefit_id",
            "run_date",
        ],
        date_column=None,
    )


def get_users(project_id: str) -> pd.DataFrame:
    """Recover the User details such as email and first name."""
    return read_raw_table_by_date(
        project_id=project_id,
        table_name="user",
        columns=[
            "id",
            "first_name",
            "email",
            "run_date",
        ],
        date_column=None,
    )


def get_customers(project_id: str) -> pd.DataFrame:
    """Recover all the quotes with benefits which we will join on the policies."""
    return read_raw_table_by_date(
        project_id=project_id,
        table_name="customer",
        columns=[
            "version_id",
            "customer_id",
            "user_id",
            "run_date",
        ],
        date_column=None,
    )


def is_qualified(run_date: date, lockin_period_days: int, start_date: date):
    days = (run_date - start_date).days
    include = days == lockin_period_days
    return include


def date_or_empty(when: datetime) -> str:
    return when.date().isoformat() if when else ""


def generate_daily_napo_benefit_code_report(
    run_date: datetime,
    project_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    output_to_file: Optional[bool] = False,
) -> str:
    """Generate the qualifying customers for the given run date."""
    log = logging.getLogger(f"{__name__}.generate_daily_napo_benefit_code_report")

    lockin_period_days = 10
    log.info(f"The lockin period in days is '{lockin_period_days}'")

    # Join customer on user table to get the first name and email:
    users = get_users(project_id=project_id)
    customers = get_customers(project_id=project_id)
    # get the latest version of the customer, ignore its history:
    customers = filter_by_version_id(customers, "customer_id")
    customers = customers.merge(users, how="left", left_on="user_id", right_on="id")
    customers = customers[["customer_id", "first_name", "email"]]

    # Join to get the quotes with the napo benefit code they have used:
    napobenefitcodes = get_napobenefitcode(
        project_id=project_id, run_date=run_date.date().isoformat()
    )
    quotewithbenefits = get_quotewithbenefit(project_id=project_id)
    quotewithbenefits = quotewithbenefits.merge(
        napobenefitcodes, how="left", left_on="benefit_id", right_on="id"
    )
    quotewithbenefits = quotewithbenefits[["quote_id", "code"]]

    # Now get the policies and filter out cancelled or inactive policies:
    df0 = get_policies(project_id=project_id)
    log.info(f"Recovering policies for project '{project_id}'")
    # get the latest version of the policy, ignore its history:
    df0 = filter_by_version_id(df0, "policy_id")
    df0["cancel_date"] = pd.to_datetime(df0["cancel_date"], unit="ms", utc=True)
    log.info(f"Policies before filtering: '{df0.count()}'")
    # Filter to policies that have not be cancelled:
    df0 = df0[df0.cancel_date.isna()]
    log.info(f"Policies after removing cancelled: '{df0.count()}'")
    # Filter to policies that are active
    df0 = df0[(df0.subscription_id.notnull()) | (df0.annual_payment_id.notnull())]
    log.info(f"Policies after removing inactive: '{df0.count()}'")

    # Now join them to the customers and quotes with codes:
    df1 = df0.merge(quotewithbenefits, how="inner", on="quote_id")
    df1 = df1.merge(customers, how="inner", on="customer_id")
    df1["start_date"] = pd.to_datetime(df1["start_date"], unit="ms", utc=True)
    df1["qualify_date"] = df1.start_date + timedelta(days=lockin_period_days)
    log.info(f"Entries before removing unqualified: '{df1.count()}'")
    # Filter to policies that qualify for the napo beneift code on the run_date:
    df1["is_qualified"] = df1.apply(
        lambda row: is_qualified(run_date, lockin_period_days, row.start_date), axis=1
    )
    df1 = df1[df1.is_qualified == True]

    # TODO:
    #
    # Exclude customers who have other policies with us already.
    #
    # Exclude customers that haven't paid their annual amount
    # Exclude customers that haven't got up to date monthly payments

    log.info(f"Entries after removing unqualified: '{df1.count()}'")
    report_fields = [
        "policy_id",
        "customer_id",
        "reference_number",
        "start_date",
        "qualify_date",
        "code",
        "first_name",
        "email",
        "quote_id",
        "annual_payment_id",
        "subscription_id",
    ]
    report = df1[report_fields]
    report["start_date"] = report["start_date"].apply(lambda d: date_or_empty(d))
    report["qualify_date"] = report["qualify_date"].apply(lambda d: date_or_empty(d))
    log.info(f"Total report entries: '{df1.count()}'")

    if report.empty:
        raise ValueError(f"No customers qualify for run date '{run_date}'!")

    csv_data = report.to_csv(header=False, index=False, quoting=csv.QUOTE_ALL)

    if output_to_file:
        filename = f"{run_date.date().isoformat()}_napo_benefit_code_daily.csv"
        log.info(f"Writing to local files: '{filename}'")
        with open(filename, "w") as fd:
            fd.write(csv_data)

    else:
        log.info(
            f"Uploading to BigQuery Project: '{project_id}' Bucket: '{gcs_bucket}' "
            f"Table '{gcs_prefix}'"
        )
        load_csv_to_gcs_bucket(gcs_bucket, csv_data, run_date, gcs_prefix, project_id)

    return csv_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        help="Query run date in UTC (inclusive)",
        required=True,
    )
    parser.add_argument(
        "--project_id",
        type=str,
        help="The google project of the data warehouse to run against.",
        required=True,
    )
    parser.add_argument(
        "--gcs_bucket",
        type=str,
        help="The base GCP bucket to upload into.",
        default="data-warehouse-harbour",
        required=False,
    )
    parser.add_argument(
        "--gcs_prefix",
        type=str,
        help="Where the table will be created.",
        default="reporting/napo_benefit_code_daily",
        required=False,
    )
    parser.add_argument(
        "--output_to_file",
        action="store_true",
        help="Write the report to the local disk instead of uploading to BigQuery.",
        default=False,
    )
    args = parser.parse_args()
    generate_daily_napo_benefit_code_report(**vars(args))
