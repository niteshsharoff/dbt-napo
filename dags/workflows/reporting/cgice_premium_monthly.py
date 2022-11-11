import argparse
import csv
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
import pytz
from google.cloud import storage

from .cgice.source import get_policies, get_dimension_tables, get_renewals_quote_source
from .cgice.transform import transform_policy_records, format_policy_records

GCS_BUCKET = "data-warehouse-harbour"


def load_csv_to_gcs_bucket(
    bucket_name: str,
    csv_str: str,
    run_date: datetime,
    prefix: str,
    filename: str = "data.csv",
) -> None:
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(
        "{prefix}/run_date={run_date}/{file_name}".format(
            prefix=prefix,
            run_date=run_date.strftime("%Y-%m-%d"),
            file_name=filename,
        )
    ).upload_from_string(csv_str, "text/csv")


def filter_by_version_id(df: pd.DataFrame, key: str) -> pd.DataFrame:
    df["latest_version"] = df.groupby(key).version_id.transform(np.max)
    df = df[df.version_id == df.latest_version]
    return df


def process_created_policies(
    changed_policies: pd.DataFrame,
    products: pd.DataFrame,
    renewals: pd.DataFrame,
    pets: pd.DataFrame,
    customers: pd.DataFrame,
    quote_sources: pd.DataFrame,
    window: Tuple[datetime, datetime],
) -> pd.DataFrame:
    df = changed_policies
    t1 = window[0].timestamp() * 1000
    t2 = window[1].timestamp() * 1000

    # preprocess
    df = df[(df.is_sold == True) & (df.created_date >= t1) & (df.created_date < t2)]
    df = filter_by_version_id(df, "policy_id")

    if df.empty:
        return pd.DataFrame()

    # denormalize
    df = df.merge(customers, how="left", on="customer_id")
    df = df.merge(pets, how="left", on="pet_id")
    df = df.merge(products, how="left", on="product_id")

    # transform
    df = transform_policy_records(df, renewals, quote_sources)
    return format_policy_records(df)


def process_cancelled_policies(
    cancelled_policies: pd.DataFrame,
    changed_policies: pd.DataFrame,
    products: pd.DataFrame,
    renewals: pd.DataFrame,
    pets: pd.DataFrame,
    customers: pd.DataFrame,
    quote_sources: pd.DataFrame,
    window: Tuple[datetime, datetime],
) -> pd.DataFrame:
    df = pd.concat([cancelled_policies, changed_policies])
    t1 = window[0].timestamp() * 1000
    t2 = window[1].timestamp() * 1000

    # preprocess
    df = df.drop_duplicates()
    df = filter_by_version_id(df, "policy_id")
    df = df[
        (df.is_sold == True)
        & (df.is_cancelled == True)
        & (df.cancel_date >= t1)
        & (df.cancel_date < t2)
    ]

    if df.empty:
        return pd.DataFrame()

    # join
    df = df.merge(customers, how="left", on="customer_id")
    df = df.merge(pets, how="left", on="pet_id")
    df = df.merge(products, how="left", on="product_id")

    # transform
    df = transform_policy_records(df, renewals, quote_sources)
    return format_policy_records(df)


def generate_monthly_premium_bdx(
    start_date: datetime,
    end_date: datetime,
) -> None:
    report_window = (start_date, end_date)

    # Dimension tables
    products, renewals, pets, customers = get_dimension_tables(start_date, end_date)
    customers = filter_by_version_id(customers, "customer_id")
    pets = filter_by_version_id(pets, "pet_id")

    # Renewal quote sources
    quote_sources = get_renewals_quote_source()

    # Fact tables
    changed_policies = get_policies(
        renewals, "change_at", (start_date, datetime.utcnow()), report_window
    )
    cancelled_policies = get_policies(
        renewals, "cancel_date", report_window, report_window
    )

    created_policies = process_created_policies(
        changed_policies,
        products,
        renewals,
        pets,
        customers,
        quote_sources,
        (start_date, end_date),
    )

    # Drop changed policies that aren't cancellations
    changed_policies = changed_policies[
        changed_policies["policy_id"].isin(cancelled_policies["policy_id"])
    ]

    cancelled_policies = process_cancelled_policies(
        cancelled_policies,
        changed_policies,
        products,
        renewals,
        pets,
        customers,
        quote_sources,
        (start_date, end_date),
    )

    report = pd.concat([created_policies, cancelled_policies])

    # Format and upload report
    if report.empty:
        raise Exception(
            "No policies found between {} - {}!".format(start_date, end_date)
        )

    report = report.sort_values(by=["Status", "Policy Id"], ascending=[False, True])
    load_csv_to_gcs_bucket(
        GCS_BUCKET,
        report.to_csv(index=False, quoting=csv.QUOTE_ALL),
        start_date,
        "{dataset_name}/{table_name}".format(
            dataset_name="reporting", table_name="cgice_premium_monthly"
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=pytz.UTC),
        help="Query start date in UTC (inclusive)",
        required=True,
    )
    parser.add_argument(
        "--end_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=pytz.UTC),
        help="Query end date in UTC (exclusive)",
        required=True,
    )
    args = parser.parse_args()
    generate_monthly_premium_bdx(**vars(args))
