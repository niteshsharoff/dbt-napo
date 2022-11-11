import logging
from datetime import datetime
from typing import Any, Optional, List, Tuple

import pandas as pd
import psycopg2
from google.cloud import storage
from jinja2 import Environment, FileSystemLoader

JINJA_ENV = Environment(loader=FileSystemLoader("dags/sql/"))
UPLOAD_PATH = "{prefix}/run_date={run_date}/{file_name}"


def query_pg(
    query: str,
    pg_host: str,
    pg_database: str,
    pg_user: str,
    pg_password: str,
) -> Optional[Tuple[List[Any], List[Tuple[Any, ...]]]]:
    with psycopg2.connect(
        host=pg_host,
        database=pg_database,
        user=pg_user,
        password=pg_password,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            return column_names, rows


def load_json_to_gcs_bucket(
    project_id: str,
    bucket_name: str,
    json_str: str,
    run_date: datetime,
    prefix: str,
    filename: str = "data.json",
) -> None:
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(
        UPLOAD_PATH.format(
            prefix=prefix,
            run_date=run_date.strftime("%Y-%m-%d"),
            file_name=filename,
        )
    ).upload_from_string(json_str, "application/json")


def load_pg_table_to_gcs(
    pg_host: str,
    pg_database: str,
    pg_user: str,
    pg_password: str,
    pg_table: str,
    pg_columns: List[str],
    dataset_name: str,
    project_id: str,
    gcs_bucket: str,
    start_date: datetime,
    end_date: datetime,
    date_column: str,
) -> None:
    log = logging.getLogger(__name__)
    column_names, rows = query_pg(
        JINJA_ENV.get_template("query_columns_by_date.sql").render(
            dict(
                columns=pg_columns,
                table_name=pg_table,
                date_column=date_column,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )
        ),
        pg_host,
        pg_database,
        pg_user,
        pg_password,
    )
    if rows:
        # Set dataframe dtype=object to prevent pandas from coercing int -> float
        df = pd.DataFrame(rows, columns=column_names, dtype=object)
        json_str = df.to_json(orient="records", lines=True)
        load_json_to_gcs_bucket(
            project_id,
            gcs_bucket,
            json_str,
            start_date,
            "{}/{}".format(dataset_name, pg_table),
        )
    else:
        log.warning(
            "No data found in table '{table_name}' on '{run_date}'".format(
                table_name=pg_table,
                run_date=start_date.strftime("%Y-%m-%d"),
            )
        )
