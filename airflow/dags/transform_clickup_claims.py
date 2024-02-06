from datetime import timedelta
import pandas as pd
import pendulum

from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.python import task
from dags.clickup_claims_export import GCS_RAW_FOLDER
from dags.workflows.common import gcs_csv_to_dataframe
from dags.workflows.convert_clickup_claim_tasks_to_claims import (
    convert_clickup_claim_tasks_to_claims,
)
from dags.workflows.create_bq_external_table import create_external_bq_table
from airflow.sensors.external_task import ExternalTaskSensor

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
        pattern=f"*/archived_claims*.csv",
    )
    vet_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{vet_claims_folder}/snapshot_date={run_date}",
        pattern=f"*/claims*.csv",
    )
    claims_folder = f"raw/clickup_claims_snapshot/{GCS_DATA_VERSION}"
    archived_customer_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{claims_folder}/snapshot_date={run_date}",
        pattern=f"*/archived_claims*.csv",
    )
    customer_claims = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{claims_folder}/snapshot_date={run_date}",
        pattern=f"*/claims*.csv",
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
    catchup=False,
    default_args={"retries": 0},
    max_active_runs=1,
    max_active_tasks=8,
    tags=["raw", "claims_snapshot"],
)
def clickup_claims_transform():
    run_date = "{{ data_interval_end.date() }}"
    claim_data_check = ExternalTaskSensor(
        task_id="clickup_claims_export_dependency_check",
        # task_id in dags/clickup_claims_export.py
        external_dag_id="clickup_claims_export",
        external_task_id="load_clickup_claims_snapshot",
        # fail after 5 minutes
        timeout=300,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        # 0 6 * * *
        execution_delta=timedelta(hours=2),
    )
    vet_claim_data_check = ExternalTaskSensor(
        task_id="clickup_vet_claims_export_dependency_check",
        # task_id in dags/clickup_vet_claims_export.py
        external_dag_id="clickup_vet_claims_export",
        external_task_id="load_vet_clickup_claims_snapshot",
        # fail after 5 minutes
        timeout=300,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        # 0 5 * * *
        execution_delta=timedelta(hours=3),
    )
    t1 = transform_clickup_claims_to_claims()
    t2 = create_claims_snapshot_table()
    ([claim_data_check, vet_claim_data_check] >> t1 >> t2)


clickup_claims_transform()
