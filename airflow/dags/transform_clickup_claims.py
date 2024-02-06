import pandas as pd
import pendulum

from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.python import task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from dags.clickup_claims_export import GCS_RAW_FOLDER
from dags.workflows.common import gcs_csv_to_dataframe
from dags.workflows.convert_clickup_claim_tasks_to_claims import (
    convert_clickup_claim_tasks_to_claims,
)
from dags.workflows.create_bq_external_table import create_external_bq_table

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCS_DATA_VERSION = "1.0.0"
GCS_SNAPSHOT_VERSION = "v1.1.0"


@task(trigger_rule="none_failed")
def transform_clickup_claims_to_claims(
    data_interval_end: pendulum.datetime = None,
):
    run_date = data_interval_end.date()
    vet_claims_folder = f"raw/clickup_vet_claims_snapshot/{GCS_DATA_VERSION}"
    archived_vet_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{vet_claims_folder}/snapshot_date={run_date}",
        pattern=f"*/archived_claims_*.csv",
    )
    vet_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{vet_claims_folder}/snapshot_date={run_date}",
        pattern=f"*/claims_*.csv",
    )
    claims_folder = f"raw/clickup_claims_snapshot/{GCS_DATA_VERSION}"
    archived_customer_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{claims_folder}/snapshot_date={run_date}",
        pattern=f"*/archived_claims_*.csv",
    )
    customer_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{claims_folder}/snapshot_date={run_date}",
        pattern=f"*/claims_*.csv",
    )

    vet_clickup_claims_df = pd.concat([vet_claims, archived_vet_claims])
    customer_clickup_claims_df = pd.concat([customer_claims, archived_customer_claims])

    # Transform tasks to claims
    claims_df = convert_clickup_claim_tasks_to_claims(
        customer_clickup_claims_df, vet_clickup_claims_df
    )

    # Load claims
    claims_df.to_csv(
        f"{GCS_RAW_FOLDER}/claims_snapshot/{GCS_SNAPSHOT_VERSION}/snapshot_date={run_date}/claims.csv",
        index=False,
    )


@task(trigger_rule="none_failed")
def create_claims_snapshot_table():
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name="raw",
        table_name="claims_snapshot_v2",
        schema_path=f"dags/schemas/raw/claims/bq_schema.json",
        source_uri=f"{GCS_RAW_FOLDER}/claims_snapshot/{GCS_SNAPSHOT_VERSION}/*",
        partition_uri=f"{GCS_RAW_FOLDER}/claims_snapshot/{GCS_SNAPSHOT_VERSION}",
        source_format="CSV",
        skip_leading_rows=1,
        partition_key="snapshot_date",
    )


@dag(
    dag_id="clickup_claims_transform",
    start_date=pendulum.datetime(2023, 4, 24, tz="UTC"),
    schedule_interval="0 8 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    max_active_tasks=8,
    tags=["raw", "claims_snapshot"],
)
def clickup_claims_transform():
    run_date = "{{ data_interval_end.date() }}"
    t1 = transform_clickup_claims_to_claims()
    t2 = create_claims_snapshot_table()
    (
        [
            GCSObjectExistenceSensor(
                task_id=f"{folder}_{file}",
                bucket=GCS_BUCKET,
                object=f"raw/{folder}/{GCS_DATA_VERSION}/snapshot_date={run_date}/{file}",
                mode="poke",
                poke_interval=5 * 60,
                timeout=60 * 60,
            )
            for folder, file in [
                ("clickup_claims_snapshot", "claims.csv"),
                ("clickup_claims_snapshot", "archived_claims.csv"),
                ("clickup_vet_claims_snapshot", "claims.csv"),
                ("clickup_vet_claims_snapshot", "archived_claims.csv"),
            ]
        ]
        >> t1
        >> t2
    )


clickup_claims_transform()
