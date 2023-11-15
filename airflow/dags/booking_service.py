from datetime import datetime

import pendulum

from airflow.decorators import task
from airflow.macros import ds_add
from airflow.models import Variable
from airflow.models.dag import dag
from dags.workflows.create_bq_external_table import create_external_bq_table
from dags.workflows.export_pg_tables_to_gcs import load_pg_table_to_gcs
from dags.workflows.move_gcs_object import move_gcs_blob
from dags.workflows.validate_json_schema import validate_json

PG_HOST = Variable.get("PG_HOST")
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BOOKING_DB_DATABASE = Variable.get("BOOKING_DB_DATABASE")
BOOKING_DB_USER = Variable.get("BOOKING_DB_USER")
BOOKING_DB_PASS = Variable.get("BOOKING_DB_PASSWORD")
BOOKING_DB_PORT = Variable.get("BOOKING_DB_PORT")
GCP_REGION = Variable.get("GCP_REGION")


@task
def export_pg_table_to_gcs(src_table: str, date_column: str, ds=None):
    load_pg_table_to_gcs(
        pg_host=PG_HOST,
        pg_port=BOOKING_DB_PORT,
        pg_database=BOOKING_DB_DATABASE,
        pg_user=BOOKING_DB_USER,
        pg_password=BOOKING_DB_PASS,
        pg_table=src_table,
        pg_columns=["*"],
        project_id=GCP_PROJECT_ID,
        dataset_name="tmp",
        gcs_bucket=GCS_BUCKET,
        start_date=datetime.strptime(ds, "%Y-%m-%d"),
        end_date=datetime.strptime(ds_add(ds, 1), "%Y-%m-%d"),
        date_column=date_column,
    )


@task
def validate_raw_data(src_table: str, dst_table: str, version: str, ds=None):
    validate_json(
        project_name=GCP_PROJECT_ID,
        bucket_name=GCS_BUCKET,
        object_path=f"tmp/{src_table}/run_date={ds}/data.json",
        schema_path=f"dags/schemas/raw/{dst_table}/{version}/validation.json",
    )


@task
def commit_raw_data(src_table: str, dataset_name: str, version: str, ds=None):
    move_gcs_blob(
        project_name=GCP_PROJECT_ID,
        src_bucket=GCS_BUCKET,
        src_path=f"tmp/{src_table}/run_date={ds}/data.json",
        dst_bucket=GCS_BUCKET,
        dst_path=f"{dataset_name}/{src_table}/{version}/run_date={ds}/data.json",
    )


@task
def create_bq_table(
    src_table: str, dst_table: str, schema_path: str, schema_version: str
):
    dataset = "raw"
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=dataset,
        table_name=dst_table,
        schema_path=schema_path,
        source_uri=f"gs://{GCS_BUCKET}/raw/{src_table}/*",
        partition_uri=f"gs://{GCS_BUCKET}/raw/{src_table}/{schema_version}",
        source_format="JSON",
    )


@dag(
    dag_id="booking_service_data_export",
    start_date=pendulum.datetime(2022, 8, 15, tz="UTC"),
    schedule_interval="@daily",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=7,
    max_active_tasks=128,
    tags=["raw", "booking_service"],
)
def export_booking_service_data():
    # Booking service booking tables to GCS
    t1 = export_pg_table_to_gcs.override(task_id="export_customer_pg_table_to_gcs")(
        "customer", "created_at"
    )
    t2 = validate_raw_data.override(task_id="validate_customer_data")(
        "customer", "booking_service_customer", "1.0.0"
    )
    t3 = commit_raw_data.override(task_id="commit_customer_data")(
        "customer", "raw", "1.0.0"
    )
    t4 = create_bq_table.override(task_id="create_customer_bq_table")(
        "customer",
        "booking_service_customer",
        "dags/schemas/raw/booking_service_customer/1.0.0/bq_schema.json",
        "1.0.0",
    )

    t1 >> t2 >> t3 >> t4

    # Booking service customer tables to BQ
    t5 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "booking", "updated_at"
    )
    t6 = validate_raw_data.override(task_id="validate_booking_data")(
        "booking", "booking_service_booking", "1.0.0"
    )
    t7 = commit_raw_data.override(task_id="commit_booking_data")(
        "booking", "raw", "1.0.0"
    )
    t8 = create_bq_table.override(task_id="create_booking_bq_table")(
        "booking",
        "booking_service_booking",
        "dags/schemas/raw/booking_service_booking/1.0.0/bq_schema.json",
        "1.0.0",
    )

    t5 >> t6 >> t7 >> t8


export_booking_service_data()
