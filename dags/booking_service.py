from datetime import datetime

import pendulum
from airflow.models import Variable
from airflow.macros import ds_add
from airflow.decorators import task
from airflow.models.dag import dag

from dags.workflows.create_bq_external_table import create_external_bq_table
from dags.workflows.export_pg_tables_to_gcs import load_pg_table_to_gcs

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
        dataset_name="raw",
        gcs_bucket=GCS_BUCKET,
        start_date=datetime.strptime(ds, "%Y-%m-%d"),
        end_date=datetime.strptime(ds_add(ds, 1), "%Y-%m-%d"),
        date_column=date_column,
    )


@task
def create_bq_table(src_table: str, dst_table: str, schema_path: str):
    dataset = "raw"
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=dataset,
        table_name=dst_table,
        schema_path=schema_path,
        source_uri=f"gs://{GCS_BUCKET}/raw/{src_table}/*",
        partition_uri=f"gs://{GCS_BUCKET}/raw/{src_table}",
        source_format="JSON",
    )


@dag(
    dag_id="booking_service_data_export",
    start_date=pendulum.datetime(2023, 4, 9, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 0},
    max_active_runs=7,
    max_active_tasks=128,
    tags=["raw", "booking_service"],
)
def export_booking_service_data():
    # Booking service database tables to GCS
    t1 = export_pg_table_to_gcs.override(task_id="export_customer_pg_table_to_gcs")(
        "customer", "created_at"
    )
    t2 = create_bq_table.override(task_id="create_customer_bq_table")(
        "customer",
        "booking_service_customer",
        "dags/schemas/raw/booking_service_customer/bq_schema.json",
    )

    t1 >> t2

    # Booking service database tables to BQ
    t3 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "booking", "updated_at"
    )
    t4 = create_bq_table.override(task_id="create_booking_bq_table")(
        "booking",
        "booking_service_booking",
        "dags/schemas/raw/booking_service_booking/bq_schema.json",
    )

    t3 >> t4


export_booking_service_data()
