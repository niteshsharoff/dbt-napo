"""Reporting helper functions other reports share.
"""
import datetime

import numpy as np
import pandas as pd
from google.cloud import storage
from jinja2 import Environment, FileSystemLoader


JINJA_ENV = Environment(loader=FileSystemLoader("dags/sql/"))


def load_csv_to_gcs_bucket(
    bucket_name: str,
    csv_str: str,
    run_date: datetime,
    prefix: str,
    project_id: str,
    filename: str = "data.csv",
) -> None:
    """Load the generate CSV up to google cloud storage."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(
        "{prefix}/run_date={run_date}/{file_name}".format(
            prefix=prefix,
            run_date=run_date.strftime("%Y-%m-%d"),
            file_name=filename,
        )
    ).upload_from_string(csv_str, "text/csv")


def filter_by_version_id(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Recover the most recent version of a ledger table entry row."""
    df["latest_version"] = df.groupby(key).version_id.transform(np.max)
    df = df[df.version_id == df.latest_version]
    return df


def read_raw_table_by_date(
    project_id: str, table_name: str, columns: list, **extra_args
) -> pd.DataFrame:
    """Read a postgresql table loaded into the raw bigquery dataset found in
    the given google project.
    """
    return pd.read_gbq(
        JINJA_ENV.get_template("query_columns_by_date.sql").render(
            dict(
                table_name=f"{project_id}.raw.{table_name}",
                columns=columns,
                **extra_args,
            )
        ),
        project_id=project_id,
    )
