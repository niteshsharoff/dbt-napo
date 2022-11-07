from datetime import datetime
from datetime import timezone

from airflow import DAG, macros
from airflow.decorators import task, task_group
from airflow.models import Variable

from workflows.create_bq_external_table import create_external_bq_table
from workflows.create_bq_view import create_napo_benefit_weekly_view
from workflows.reporting.napo_benefit_code_daily import (
    generate_daily_napo_benefit_code_report,
)

GCP_CREDENTIALS = Variable.get("GCP_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

DATASET_PREFIX = Variable.get("DATASET_PREFIX", default_var="oisin")
PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")


@task(task_id="generate_daily_report")
def generate_report(ds=None):
    """Generate the daily napo benefit report which shows who qualifies on
    each run date.
    """
    run_date = macros.datetime.strptime(ds, "%Y-%m-%d")
    run_date = run_date.replace(tzinfo=timezone.utc)
    generate_daily_napo_benefit_code_report(
        run_date=run_date,
        project_id=GCP_PROJECT_ID,
        bucket="data-warehouse-harbour",
        table_name="reporting/napo_benefit_code_daily",
    )


@task(task_id="create_daily_table")
def create_table(dataset_name: str, table_name: str, ds=None):
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=dataset_name,
        table_name=table_name,
        source_uri=f"gs://{GCS_BUCKET}/{dataset_name}/{table_name}/*",
        partition_uri=f"gs://{GCS_BUCKET}/{dataset_name}/{table_name}",
        source_format="CSV",
        schema_path="dags/schemas/csv/napo_benefit_code_daily/1.0.0/bq_schema.json",
        skip_leading_rows=1,
    )


@task(task_id="update_weekly_view")
def create_view(dataset_name: str, table_name: str, view_name: str, ds=None):
    create_napo_benefit_weekly_view(
        GCP_PROJECT_ID,
        dataset_name,
        table_name,
        view_name,
        datetime.strptime(ds, "%Y-%m-%d"),
    )


with DAG(
    dag_id="napo_benefit_code",
    # The begining of time for Napo Benefit Codes:
    start_date=datetime(2022, 8, 23),
    schedule_interval="0 0 * * *",
    catchup=True,
    default_args={"retries": 0},
) as dag:

    @task_group(group_id="big_query")
    def create_table_and_view():
        t1 = create_table("reporting", "napo_benefit_code_daily")
        t2 = create_view(
            "reporting",
            "napo_benefit_code_daily",
            "napo_benefit_code_weekly",
        )
        t1 >> t2

    generate_report() >> create_table_and_view()
