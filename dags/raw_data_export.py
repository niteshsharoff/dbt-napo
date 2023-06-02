from datetime import datetime
from enum import Enum, auto

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import task, task_group
from airflow.macros import ds_add
from airflow.models import Variable
from airflow.models.dag import dag

from workflows.create_bq_external_table import create_external_bq_table
from workflows.export_pg_tables_to_gcs import load_pg_table_to_gcs
from workflows.move_gcs_object import move_gcs_blob
from workflows.validate_json_schema import validate_json

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
PG_HOST = Variable.get("PG_HOST")
POLICY_DATABASE = Variable.get("POLICY_DATABASE")
POLICY_DB_USER = Variable.get("POLICY_DB_USER")
POLICY_DB_PASS = Variable.get("POLICY_DB_PASSWORD")
QUOTE_DATABASE = Variable.get("QUOTE_DATABASE")
QUOTE_DB_USER = Variable.get("QUOTE_DB_USER")
QUOTE_DB_PASS = Variable.get("QUOTE_DB_PASSWORD")


class DataSource(Enum):
    POLICY_DB = auto()
    QUOTE_DB = auto()


@task
def export_pg_table_to_gcs(
    source: DataSource, src_table: str, date_column: str, ds=None
):
    pg_database = QUOTE_DATABASE if source == DataSource.QUOTE_DB else POLICY_DATABASE
    pg_user = QUOTE_DB_USER if source == DataSource.QUOTE_DB else POLICY_DB_USER
    pg_password = QUOTE_DB_PASS if source == DataSource.QUOTE_DB else POLICY_DB_PASS
    load_pg_table_to_gcs(
        pg_host=PG_HOST,
        pg_database=pg_database,
        pg_user=pg_user,
        pg_password=pg_password,
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
        schema_path=f"dags/schemas/postgres/{dst_table}/{version}/validation.json",
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
def create_bq_external_table(
    src_table: str,
    dataset: str,
    dst_table: str,
    version: str,
    load_last_partition_only: bool = False,
    ds=None,
):
    if load_last_partition_only:
        src_uri = f"gs://{GCS_BUCKET}/{dataset}/{src_table}/{version}/run_date={ds}/*"
    else:
        src_uri = f"gs://{GCS_BUCKET}/{dataset}/{src_table}/{version}/*"

    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=dataset,
        table_name=dst_table,
        schema_path=f"dags/schemas/postgres/{dst_table}/{version}/bq_schema.json",
        source_uri=src_uri,
        partition_uri=f"gs://{GCS_BUCKET}/{dataset}/{src_table}/{version}",
        source_format="JSON",
    )


def create_pipeline(
    source: DataSource,
    src_table: str,
    dataset_name: str,
    dst_table: str,
    version: str,
    pg_date_column: str,
    load_last_partition_only: bool = False,
):
    t1 = export_pg_table_to_gcs(source, src_table, pg_date_column)
    t2 = validate_raw_data(src_table, dst_table, version)
    t3 = commit_raw_data(src_table, dataset_name, version)
    t4 = create_bq_external_table.override(
        outlets=[Dataset(f"{dataset_name}.{dst_table}")]
    )(
        src_table,
        dataset_name,
        dst_table,
        version,
        load_last_partition_only,
    )

    return t1 >> t2 >> t3 >> t4


@dag(
    dag_id="policy_service_data_export",
    start_date=pendulum.datetime(2021, 9, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=7,
    max_active_tasks=128,
    tags=["raw", "policy_service"],
)
def export_policy_service_data():
    # Policy service database tables
    schema_version = "1.0.0"
    for pg_table, bq_dataset, bq_table, pg_date_column, load_last_partition_only in [
        ("policy_policy", "raw", "policy", "change_at", False),
        ("policy_pet", "raw", "pet", "change_at", False),
        ("policy_breed", "raw", "breed", None, False),
        ("policy_subscription", "raw", "subscription", "modified_date", False),
        ("policy_renewal", "raw", "renewal", "updated_at", False),
        ("policy_customer", "raw", "customer", "change_at", False),
        ("auth_user", "raw", "user", "date_joined", False),
        ("policy_product", "raw", "product", None, True),
        ("policy_napobenefitcode", "raw", "napobenefitcode", None, False),
        ("policy_quotewithbenefit", "raw", "quotewithbenefit", "created_date", False),
        ("policy_activatedreferral", "raw", "activatedreferral", "created_at", False),
        ("policy_vouchercode", "raw", "vouchercode", "created_date", False),
    ]:

        @task_group(group_id=f"policy_{bq_table}")
        def create_policy_pipeline():
            create_pipeline(
                DataSource.POLICY_DB,
                pg_table,
                bq_dataset,
                bq_table,
                schema_version,
                pg_date_column,
                load_last_partition_only,
            )

        create_policy_pipeline()


export_policy_service_data()


@dag(
    dag_id="quote_service_data_export",
    start_date=pendulum.datetime(2021, 10, 17, tz="UTC"),
    schedule_interval="@daily",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=7,
    max_active_tasks=128,
    tags=["raw", "quote_service"],
)
def export_quote_service_data():
    # Quote service database tables
    for pg_table, bq_dataset, bq_table, schema_version, pg_date_column in [
        ("quote_quoterequest", "raw", "quoterequest", "1.0.0", "created_at"),
    ]:

        @task_group(group_id=f"quote_{bq_table}")
        def create_quote_pipeline():
            create_pipeline(
                DataSource.QUOTE_DB,
                pg_table,
                bq_dataset,
                bq_table,
                schema_version,
                pg_date_column,
            )

        create_quote_pipeline()


export_quote_service_data()
