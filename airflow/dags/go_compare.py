import pendulum

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from dags.workflows.create_bq_external_table import create_external_bq_table
from dags.workflows.create_bq_view import create_pcw_sales_view
from dags.workflows.export_bq_result_to_gcs import export_query_to_gcs
from dags.workflows.reporting.gocompare.utils import (
    get_monthly_report_name,
    get_weekly_report_name,
)

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = "reporting"
BQ_SCHEMA_PATH = "dags/schemas/reporting/gocompare_sales_report/bq_schema.json"

DAILY_TABLE = "gocompare_sales_report_daily"
WEEKLY_TABLE = "gocompare_sales_report_weekly"
MONTHLY_TABLE = "gocompare_sales_report_monthly"


@task(outlets=[Dataset(f"{BQ_DATASET}.{DAILY_TABLE}")])
def create_daily_table():
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=BQ_DATASET,
        table_name=DAILY_TABLE,
        source_uri=f"gs://{GCS_BUCKET}/tmp/gocompare/*",
        partition_uri=f"gs://{GCS_BUCKET}/tmp/gocompare",
        source_format="CSV",
        schema_path=BQ_SCHEMA_PATH,
        skip_leading_rows=1,
    )


@task.branch
def weekly_branch(data_interval_end: pendulum.datetime = None):
    if data_interval_end.day_of_week == 1:
        return "create_weekly_view"

    return "no_op"


@task(outlets=[Dataset(f"{BQ_DATASET}.{WEEKLY_TABLE}_*")])
def create_weekly_view(data_interval_end: pendulum.datetime = None):
    run_date = data_interval_end.replace(hour=0)
    start_date = run_date.subtract(days=7)
    create_pcw_sales_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=DAILY_TABLE,
        view_name=f"{WEEKLY_TABLE}_{start_date.format('YYYYMMDD')}",
        start_date=start_date,
        end_date=run_date,
    )


@task
def export_weekly_report(data_interval_end: pendulum.datetime = None):
    """
    This task exports the query results of a weekly view to Cloud Storage.
    """
    run_date = data_interval_end.replace(hour=0)
    start_date = run_date.subtract(days=7)
    view_name = f"{WEEKLY_TABLE}_{start_date.format('YYYYMMDD')}"
    gcs_file_name = get_weekly_report_name(run_date)
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{view_name}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/{}/run_date={}/{}".format(
            BQ_DATASET,
            WEEKLY_TABLE,
            run_date.date(),
            gcs_file_name,
        ),
    )


@task.branch
def monthly_branch(data_interval_end: pendulum.datetime = None):
    if data_interval_end.day == 1:
        return "create_monthly_view"

    return "no_op"


@task(outlets=[Dataset(f"{BQ_DATASET}.{WEEKLY_TABLE}_*")])
def create_monthly_view(data_interval_end: pendulum.datetime = None):
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    create_pcw_sales_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=DAILY_TABLE,
        view_name=f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}",
        start_date=start_date,
        end_date=run_date,
    )


@task
def export_monthly_report(data_interval_end: pendulum.datetime = None):
    """
    This task exports the query results of a monthly view to Cloud Storage.
    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    view_name = f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}"
    gcs_file_name = get_monthly_report_name(run_date)
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{view_name}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/{}/run_date={}/{}".format(
            BQ_DATASET,
            MONTHLY_TABLE,
            run_date.date(),
            gcs_file_name,
        ),
    )


@dag(
    dag_id="go_compare",
    start_date=pendulum.datetime(2022, 12, 31, tz="UTC"),
    schedule_interval="0 2 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["reporting", "weekly", "monthly"],
)
def go_compare():
    """
    The GoCompare daily sales report is generated on a reporting instance via a Django
    management command.

    A k8s cronjob is set up on the production cluster to sync the reports to GCS daily
    at 01:10.

    This Airflow pipeline will automate Big Query table creation and export the CSV
    report to Cloud Storage.

    This pipeline is scheduled to run daily at 02:00 but weekly and monthly tasks will
    be skipped unless it's the first day of the week or month.
    """
    report_stub = EmptyOperator(task_id="report_stub")
    no_op = EmptyOperator(task_id="no_op")

    is_first_day_of_week = weekly_branch()
    is_first_day_of_month = monthly_branch()

    report_stub >> create_daily_table() >> [is_first_day_of_week, is_first_day_of_month]

    # Weekly tasks branch
    with TaskGroup(group_id="first_of_week", prefix_group_id=False) as weekly_tasks:
        create_weekly_view() >> export_weekly_report()

    is_first_day_of_week >> weekly_tasks
    is_first_day_of_week >> no_op

    # Monthly tasks branch
    with TaskGroup(group_id="first_of_month", prefix_group_id=False) as monthly_tasks:
        create_monthly_view() >> export_monthly_report()

    is_first_day_of_month >> monthly_tasks
    is_first_day_of_month >> no_op


go_compare()
