from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.macros import ds_add
from airflow.models import Variable

from workflows.create_bq_external_table import create_external_bq_table
from workflows.export_pg_tables_to_gcs import load_pg_table_to_gcs
from workflows.move_gcs_object import move_gcs_blob
from workflows.validate_json_schema import validate_json


GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")


@task(task_id="export_data_to_gcs")
def export_pg_table_to_gcs(src_table: str, date_column: str, ds=None):
    load_pg_table_to_gcs(
        pg_host=PG_HOST,
        pg_database=PG_DATABASE,
        pg_user=PG_USER,
        pg_password=PG_PASSWORD,
        pg_table=src_table,
        pg_columns=["*"],
        project_id=GCP_PROJECT_ID,
        dataset_name="tmp",
        gcs_bucket=GCS_BUCKET,
        start_date=datetime.strptime(ds, "%Y-%m-%d"),
        end_date=datetime.strptime(ds_add(ds, 1), "%Y-%m-%d"),
        date_column=date_column,
    )


@task(task_id="validate_raw_data")
def validate_raw_data(src_table: str, dst_table: str, version: str, ds=None):
    validate_json(
        bucket_name=GCS_BUCKET,
        object_path=f"tmp/{src_table}/run_date={ds}/data.json",
        schema_path=f"dags/schemas/postgres/{dst_table}/{version}/validation.json",
    )


@task(task_id="commit_raw_data")
def commit_raw_data(src_table: str, dataset_name: str, version: str, ds=None):
    move_gcs_blob(
        src_bucket=GCS_BUCKET,
        src_path=f"tmp/{src_table}/run_date={ds}/data.json",
        dst_bucket=GCS_BUCKET,
        dst_path=f"{dataset_name}/{src_table}/{version}/run_date={ds}/data.json",
    )


@task(task_id="update_bq_external_table")
def create_bq_external_table(
    src_table: str,
    dataset_name: str,
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
        dataset_name=dataset_name,
        table_name=dst_table,
        schema_path=f"dags/schemas/postgres/{dst_table}/{version}/bq_schema.json",
        source_uri=src_uri,
        partition_uri=f"gs://{GCS_BUCKET}/{dataset_name}/{src_table}/{version}",
        source_format="JSON",
    )


with DAG(
    dag_id="raw_data_export",
    start_date=datetime(2021, 9, 1),
    schedule_interval="0 0 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=32,
    max_active_tasks=128,
) as dag:
    for pg_table, dataset, bq_table, schema_version, pg_date_column in [
        ("policy_policy", "raw", "policy", "1.0.0", "change_at"),
        ("policy_pet", "raw", "pet", "1.0.0", "change_at"),
        ("policy_breed", "raw", "breed", "1.0.0", None),
        ("policy_subscription", "raw", "subscription", "1.0.0", "modified_date"),
        ("policy_renewal", "raw", "renewal", "1.1.0", "updated_at"),
        ("policy_customer", "raw", "customer", "1.0.0", "change_at"),
        ("auth_user", "raw", "user", "1.0.0", "date_joined"),
        ("policy_product", "raw", "product", "1.0.0", "created_date"),
        ("policy_napobenefitcode", "raw", "napobenefitcode", "1.0.0", None),
        ("policy_quotewithbenefit", "raw", "quotewithbenefit", "1.0.0", "created_date"),
    ]:
        @task_group(group_id=f"{bq_table}_raw_data_pipeline")
        def create_pipeline(
            src_table: str,
            dataset_name: str,
            dst_table: str,
            version: str,
            pg_date_column: str,
        ):
            t1 = export_pg_table_to_gcs(src_table, pg_date_column)
            t2 = validate_raw_data(src_table, dst_table, version)
            t3 = commit_raw_data(src_table, dataset_name, version)
            t4 = create_bq_external_table(
                src_table,
                dataset_name,
                dst_table,
                version,
                # Only load last partition if no partition column specified
                pg_date_column is None,
            )
            t1 >> t2 >> t3 >> t4

        create_pipeline(pg_table, dataset, bq_table, schema_version, pg_date_column)
