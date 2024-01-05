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

    # Creating raw.booking_service_content_completion table in GCS
    t9 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "content_completion", "updated_at"
    )

    t10 = validate_raw_data.override(task_id="validate_content_completion_data")(
        "content_completion", "booking_service_content_completion", "1.0.0"
    )

    t11 = commit_raw_data.override(task_id="commit_content_completion_data")(
        "content_completion", "raw", "1.0.0"
    )

    t12 = create_bq_table.override(task_id="create_booking_bq_table")(
        "content_completion",
        "booking_service_content_completion",
        "dags/schemas/raw/booking_service_content_completion/bq_schema.json",
        "1.0.0",
    )

    t9 >> t10 >> t11 >> t12

    # Creating raw.booking_service_pet table in GCS
    t13 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "pet", "updated_at"
    )

    t14 = validate_raw_data.override(task_id="validate_pet_data")(
        "pet", "booking_service_pet", "1.0.0"
    )

    t15 = commit_raw_data.override(task_id="commit_pet_data")(
        "pet", "raw", "1.0.0"
    )

    t16 = create_bq_table.override(task_id="create_booking_bq_table")(
        "pet",
        "booking_service_pet",
        "dags/schemas/raw/booking_service_pet/bq_schema.json",
        "1.0.0",
    )

    t13 >> t14 >> t15 >> t16

    # Creating raw.booking_service_subscription table in GCS
    t17 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "subscription", "updated_at"
    )

    t18 = validate_raw_data.override(task_id="validate_subscription_data")(
        "subscription", "booking_service_subscription", "1.0.0"
    )

    t19 = commit_raw_data.override(task_id="commit_subscription_data")(
        "subscription", "raw", "1.0.0"
    )

    # Creating raw.booking_service_subscription table in BQ
    t20 = create_bq_table.override(task_id="create_booking_bq_table")(
        "subscription",
        "booking_service_subscription",
        "dags/schemas/raw/booking_service_subscription/bq_schema.json",
        "1.0.0",
    )

    t17 >> t18 >> t19 >> t20

    # Creating raw.booking_service_topic_interest table in GCS
    t21 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "topic_interest", "updated_at"
    )

    t22 = validate_raw_data.override(task_id="validate_topic_interest_data")(
        "topic_interest", "booking_service_topic_interest", "1.0.0"
    )

    t23 = commit_raw_data.override(task_id="commit_topic_interest_data")(
        "topic_interest", "raw", "1.0.0"
    )

    # Creating raw.booking_service_topic_interest table in BQ
    t24 = create_bq_table.override(task_id="create_booking_bq_table")(
        "topic_interest",
        "booking_service_topic_interest",
        "dags/schemas/raw/booking_service_topic_interest/bq_schema.json",
        "1.0.0",
    )

    t21 >> t22 >> t23 >> t24

    # Creating raw.booking_service_topic_preference table in GCS
    t25 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "topic_preference", "updated_at"
    )

    t26 = validate_raw_data.override(task_id="validate_topic_preference_data")(
        "topic_preference", "booking_service_topic_preference", "1.0.0"
    )

    t27 = commit_raw_data.override(task_id="commit_topic_preference_data")(
        "topic_preference", "raw", "1.0.0"
    )

    # Creating raw.booking_service_topic_preference table in BQ
    t28 = create_bq_table.override(task_id="create_booking_bq_table")(
        "topic_preference",
        "booking_service_topic_preference",
        "dags/schemas/raw/booking_service_topic_preference/bq_schema.json",
        "1.0.0",
    )

    t25 >> t26 >> t27 >> t28

    # Creating raw.booking_service_purchases table in GCS
    t29 = export_pg_table_to_gcs.override(task_id="export_booking_pg_table_to_gcs")(
        "purchases", "updated_at"
    )

    t30 = validate_raw_data.override(task_id="validate_purchases_data")(
        "purchases", "booking_service_purchases", "1.0.0"
    )

    t31 = commit_raw_data.override(task_id="commit_purchases_data")(
        "purchases", "raw", "1.0.0"
    )

    # Creating raw.booking_service_purchases table in BQ
    t32 = create_bq_table.override(task_id="create_booking_bq_table")(
        "purchases",
        "booking_service_purchases",
        "dags/schemas/raw/booking_service_purchases/bq_schema.json",
        "1.0.0",
    )

    t29 >> t30 >> t31 >> t32


export_booking_service_data()
