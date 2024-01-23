import logging

import pendulum
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.python import task
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.task_group import TaskGroup
from dags.workflows.common import gcs_csv_to_dataframe
from dags.workflows.create_bq_view import create_bq_view
from dags.workflows.export_bq_result_to_gcs import export_query_to_gcs
from dags.workflows.reporting.cgice.utils import (
    get_monthly_report_name,
    get_monthly_reporting_period,
)
from dags.workflows.upload_to_google_drive import (
    file_exists_on_google_drive,
    upload_to_google_drive,
)

JINJA_ENV = Environment(loader=FileSystemLoader("dags/"))
PARTITION_INTEGRITY_CHECK = JINJA_ENV.get_template("sql/partition_integrity_check.sql")
CUMULATIVE_BDX_REPORT_QUERY = JINJA_ENV.get_template(
    "sql/cgice_premium_bdx_monthly.sql"
)

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = "data-warehouse-harbour"
BQ_DATASET = "reporting"

# https://cloud.getdbt.com/deploy/67538/projects/106847/jobs/235012
DBT_CLOUD_JOB_ID = 289269

GOOGLE_DRIVE_FOLDER_ID = "1541hzsET3OMSyc4JKlCdl3HmbK9wmXH3"


@task
def check_run_date(data_interval_end: pendulum.datetime = None):
    if (
        (data_interval_end.day_of_week == 0)
        or (data_interval_end.day == 1)
        or (data_interval_end.day == 15)
    ):
        return True
    else:
        raise AirflowSkipException


@task
def create_view_on_bq(data_interval_end: pendulum.datetime = None):
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        view_name=f"cgice_premium_bdx_monthly_{start_date.format('YYYYMMDD')}",
        view_query=CUMULATIVE_BDX_REPORT_QUERY.render(
            dict(
                start_date=start_date.format("YYYY-MM-DD"),
                end_date=end_date.format("YYYY-MM-DD"),
            )
        ),
    )


@task
def export_report_to_gcs(data_interval_end: pendulum.datetime = None):
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    view_name = f"cgice_premium_bdx_monthly_{start_date.format('YYYYMMDD')}"
    gcs_file_name = get_monthly_report_name(start_date)
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"select * from `{GCP_PROJECT_ID}.{BQ_DATASET}.{view_name}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/{}/run_date={}/{}".format(
            BQ_DATASET,
            "cgice_premium_bdx_monthly",
            start_date.date().format("YYYY-MM"),
            gcs_file_name,
        ),
        encoding="utf-16",
    )


@task
def data_integrity_check(data_interval_end: pendulum.datetime = None):
    run_date = data_interval_end
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = PARTITION_INTEGRITY_CHECK.render(
        source_tables=["policy", "customer", "pet", "quoterequest"],
        start_date=start_date.format("YYYY-MM-DD"),
        end_date=run_date.format("YYYY-MM-DD"),
    )
    logging.info("Checking source table partition integrity with query: \n" + query)
    query_job = client.query(query)
    result = query_job.result()
    if result.total_rows != 0:
        raise AirflowFailException


@task
def report_exists_on_gdrive_check(data_interval_end: pendulum.datetime = None):
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    report_name = get_monthly_report_name(start_date)
    logging.info(report_name)
    if file_exists_on_google_drive(
        file_name=report_name,
        token_file=OAUTH_TOKEN_FILE,
    ):
        raise AirflowSkipException


@task
def report_row_count_check(data_interval_end: pendulum.datetime = None):
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    filename = get_monthly_report_name(start_date)
    df = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder="{}/cgice_premium_bdx_monthly/run_date={}".format(
            BQ_DATASET,
            start_date.date().format("YYYY-MM"),
        ),
        filename=filename,
        encoding="utf-16",
    )
    logging.info(df.head())
    if df.empty:
        raise AirflowFailException


@task
def upload_report_to_gdrive(data_interval_end: pendulum.datetime = None):
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    report_name = get_monthly_report_name(start_date)
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path="{}/cgice_premium_bdx_monthly/run_date={}/{}".format(
            BQ_DATASET,
            start_date.date().format("YYYY-MM"),
            report_name,
        ),
        gdrive_folder_id=GOOGLE_DRIVE_FOLDER_ID,
        gdrive_file_name=report_name,
        token_file=OAUTH_TOKEN_FILE,
    )


@dag(
    dag_id="cgice_premium_bdx",
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),
    schedule_interval="0 4 * * *",  # 4am daily
    catchup=False,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["reporting", "daily"],
)
def cgice():
    with TaskGroup(group_id="cgice_premium_bdx_monthly", prefix_group_id=False):
        dbt_checks = DbtCloudRunJobOperator(
            task_id="dbt_checks",
            job_id=DBT_CLOUD_JOB_ID,
            check_interval=10,
            timeout=300,
            trigger_rule="one_success",
        )
        (
            check_run_date()
            >> create_view_on_bq()
            >> export_report_to_gcs()
            >> [
                data_integrity_check(),
                report_row_count_check(),
                dbt_checks,
            ]
            >> upload_report_to_gdrive()
        )


cgice()
