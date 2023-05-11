import json
import logging
from typing import Optional
from datetime import date

from airflow.operators.empty import EmptyOperator
from billiard.pool import Pool

import pandas as pd
import pendulum
import requests
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import Variable
from airflow.models.dag import dag
from google.cloud import storage

from dags.workflows.create_bq_external_table import create_external_bq_table
from dags.workflows.convert_clickup_claim_tasks_to_claims import (
    convert_clickup_claim_tasks_to_claims,
)

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCS_RAW_FOLDER_PATH = "raw"
GCS_RAW_FOLDER = "gs://{gcs_bucket}/{gcs_raw_folder_path}".format(
    gcs_bucket=GCS_BUCKET, gcs_raw_folder_path=GCS_RAW_FOLDER_PATH
)
GCS_DATA_VERSION = '1.0.0'

CLICKUP_API_URL = "https://api.clickup.com/api/v2"
CLICKUP_API_KEY = Variable.get("CLICKUP_API_KEY")
CLICKUP_LIST_ID_CLAIMS = Variable.get("CLICKUP_LIST_ID_CLAIMS")
CLICKUP_LIST_ID_VET_CLAIMS = Variable.get("CLICKUP_LIST_ID_VET_CLAIMS")
CLICKUP_JSON_FIELDS = [
    "status",
    "creator",
    "assignees",
    "watchers",
    "checklists",
    "tags",
    "priority",
    "custom_fields",
    "dependencies",
    "linked_tasks",
    "sharing",
    "list",
    "project",
    "folder",
    "space",
]


def _get_tasks(page: int, list_id: str, archived: str = "false"):
    headers = {
        "Content-Type": "application/json",
        "Authorization": CLICKUP_API_KEY,
    }
    query = {
        "archived": archived,
        "page": page,
        "subtasks": "true",
        "include_closed": "true",
    }
    response = requests.get(
        f"{CLICKUP_API_URL}/list/{list_id}/task",
        headers=headers,
        params=query,
    )
    data = response.json()
    return data.get("tasks")


def _gcs_csv_to_dataframe(
    gcs_path: str, gcs_folder: str, run_date: str, filename: str
) -> Optional[pd.DataFrame]:
    bucket = storage.Client().get_bucket(GCS_BUCKET)
    filepath = f"{gcs_path}/{gcs_folder}/snapshot_date={run_date}/{filename}"
    blobpath = f"{GCS_RAW_FOLDER_PATH}/{gcs_folder}/snapshot_date={run_date}/{filename}"

    if bucket.blob(blobpath).exists():
        return pd.read_csv(filepath)

    logging.info(f"{blobpath} does not exist!")
    return pd.DataFrame()


@task
def get_snapshot(
    list_id: str,
    gcs_folder: str,
    archived: str = "false",
    data_interval_end: pendulum.datetime = None,
):
    if data_interval_end.date() != date.today():
        logging.error("Cannot get a snapshot in the past from ClickUp API")
        raise AirflowFailException

    pool = Pool()
    max_pages = 200
    offset = 10
    page = 0
    results = []

    while True:
        page_args = [(i, list_id, archived) for i in range(page, page + offset)]
        page_results = pool.starmap(_get_tasks, page_args)
        page_results = [item for sublist in page_results for item in sublist]
        results.extend(page_results)

        page += offset

        if page >= max_pages:
            logging.error("Max page limit exceeded!")
            raise AirflowFailException

        if not page_results:
            break

    if not any(results):
        raise AirflowSkipException

    df = pd.DataFrame.from_records(results)
    logging.info(df.count())

    for column in CLICKUP_JSON_FIELDS:
        df[column] = df[column].astype(object).apply(json.dumps)

    df.to_csv(
        "{gcs_path}/snapshot_date={run_date}/{filename}.csv".format(
            gcs_path=f"{GCS_RAW_FOLDER}/{gcs_folder}/{GCS_DATA_VERSION}",
            run_date=data_interval_end.date(),
            filename="claims" if archived == "false" else "archived_claims",
        ),
        index=False,
    )


@task
def create_clickup_claims_bq_external_table(table_name: str):
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name="raw",
        table_name=table_name,
        schema_path="dags/schemas/raw/clickup_claims/bq_schema.json",
        source_uri=f"{GCS_RAW_FOLDER}/{table_name}/{GCS_DATA_VERSION}/*",
        partition_uri=f"{GCS_RAW_FOLDER}/{table_name}/{GCS_DATA_VERSION}",
        source_format="CSV",
        skip_leading_rows=1,
        partition_key="snapshot_date",
    )


@task(trigger_rule="none_failed")
def transform_clickup_claims_to_claims(
    data_interval_end: pendulum.datetime = None,
):
    run_date = data_interval_end.date()
    archived_vet_claims = _gcs_csv_to_dataframe(
        gcs_path=GCS_RAW_FOLDER,
        gcs_folder=f"clickup_vet_claims_snapshot/{GCS_DATA_VERSION}",
        run_date=run_date,
        filename="archived_claims.csv",
    )
    vet_claims = _gcs_csv_to_dataframe(
        gcs_path=GCS_RAW_FOLDER,
        gcs_folder=f"clickup_vet_claims_snapshot/{GCS_DATA_VERSION}",
        run_date=run_date,
        filename="claims.csv",
    )
    archived_customer_claims = _gcs_csv_to_dataframe(
        gcs_path=GCS_RAW_FOLDER,
        gcs_folder=f"clickup_claims_snapshot/{GCS_DATA_VERSION}",
        run_date=run_date,
        filename="archived_claims.csv",
    )
    customer_claims = _gcs_csv_to_dataframe(
        gcs_path=GCS_RAW_FOLDER,
        gcs_folder=f"clickup_claims_snapshot/{GCS_DATA_VERSION}",
        run_date=run_date,
        filename="claims.csv",
    )

    vet_clickup_claims_df = pd.concat([vet_claims, archived_vet_claims])
    customer_clickup_claims_df = pd.concat([customer_claims, archived_customer_claims])

    # Transform tasks to claims
    claims_df = convert_clickup_claim_tasks_to_claims(
        customer_clickup_claims_df, vet_clickup_claims_df
    )

    # Load claims
    claims_df.to_csv(
        "{gcs_raw_folder}/claims_snapshot/snapshot_date={run_date}/claims.csv".format(
            gcs_raw_folder=GCS_RAW_FOLDER, run_date=data_interval_end.date()
        ),
        index=False,
    )


@task(trigger_rule="none_failed")
def create_claims_snapshot_table():
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name="raw",
        table_name="claims_snapshot",
        schema_path="dags/schemas/raw/claims/bq_schema.json",
        source_uri=f"{GCS_RAW_FOLDER}/claims_snapshot/*",
        partition_uri=f"{GCS_RAW_FOLDER}/claims_snapshot",
        source_format="CSV",
        skip_leading_rows=1,
        partition_key="snapshot_date",
    )


@dag(
    dag_id="clickup_claims_export",
    start_date=pendulum.datetime(2023, 3, 28, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    max_active_tasks=8,
    tags=["raw", "clickup"],
)
def clickup_claims_export():
    no_op = EmptyOperator(task_id="no_op")

    clickup_claims_bq_table_name = "clickup_claims_snapshot"
    t1 = get_snapshot.override(task_id="load_clickup_claims_snapshot")(
        list_id=CLICKUP_LIST_ID_CLAIMS,
        gcs_folder=clickup_claims_bq_table_name,
    )
    t2 = get_snapshot.override(task_id="load_archived_clickup_claims_snapshot")(
        list_id=CLICKUP_LIST_ID_CLAIMS,
        gcs_folder=clickup_claims_bq_table_name,
        archived="true",
    )
    t3 = create_clickup_claims_bq_external_table.override(
        task_id="create_clickup_claims_snapshot_table"
    )(clickup_claims_bq_table_name)

    clickup_claims_bq_table_name = "clickup_vet_claims_snapshot"
    t4 = get_snapshot.override(task_id="load_vet_clickup_claims_snapshot")(
        list_id=CLICKUP_LIST_ID_VET_CLAIMS,
        gcs_folder=clickup_claims_bq_table_name,
    )
    t5 = get_snapshot.override(task_id="load_archived_vet_clickup_claims_snapshot")(
        list_id=CLICKUP_LIST_ID_VET_CLAIMS,
        gcs_folder=clickup_claims_bq_table_name,
        archived="true",
    )
    t6 = create_clickup_claims_bq_external_table.override(
        task_id="create_vet_clickup_claims_snapshot_table"
    )(clickup_claims_bq_table_name)

    t7 = transform_clickup_claims_to_claims()
    t8 = create_claims_snapshot_table()

    no_op >> t1 >> t3
    no_op >> t2 >> t3
    no_op >> t4 >> t6
    no_op >> t5 >> t6
    t3 >> t7
    t6 >> t7
    t7 >> t8


clickup_claims_export()
