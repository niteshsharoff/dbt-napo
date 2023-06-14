import logging

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.python import task
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from dags.workflows.create_bq_view import create_bq_view
from dags.workflows.reporting.cgice.utils import get_monthly_reporting_period, \
    get_monthly_report_name
from dags.workflows.upload_to_google_drive import file_exists_on_google_drive

JINJA_ENV = Environment(loader=FileSystemLoader("dags/"))
PARTITION_INTEGRITY_CHECK = JINJA_ENV.get_template("sql/partition_integrity_check.sql")
CUMULATIVE_BDX_REPORT_QUERY = JINJA_ENV.get_template("sql/cgice_premium_bdx.sql")

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = "data-warehouse-harbour"

# https://cloud.getdbt.com/deploy/67538/projects/106847/jobs/235012
DBT_CLOUD_JOB_ID = 289269

GOOGLE_DRIVE_FOLDER_ID = ""


@task
def create_monthly_view(data_interval_end: pendulum.datetime = None):
    start_date, end_date = get_monthly_reporting_period(data_interval_end)
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name="reporting",
        view_name=f"cgice_premium_bdx_monthly_{start_date.format('YYYYMMDD')}",
        view_query=CUMULATIVE_BDX_REPORT_QUERY.render(
            dict(
                start_date=start_date.format("YYYY-MM-DD"),
                end_date=end_date.format("YYYY-MM-DD"),
            )
        ),
    )


@task
def export_monthly_report(data_interval_end: pendulum.datetime = None):
    pass


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
    if file_exists_on_google_drive(
        file_name=report_name,
        token_file=OAUTH_TOKEN_FILE,
    ):
        raise AirflowSkipException


@task
def report_row_count_check(data_interval_end: pendulum.datetime = None):
    pass


@task
def upload_report(data_interval_end: pendulum.datetime = None):
    pass


@dag(
    dag_id="cgice",
    start_date=pendulum.datetime(2022, 12, 1, tz="UTC"),
    schedule_interval="0 3 * * *",
    catchup=False,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["reporting", "daily"],
)
def cgice():
    dbt_checks = DbtCloudRunJobOperator(
        task_id="dbt_checks",
        job_id=DBT_CLOUD_JOB_ID,
        check_interval=10,
        timeout=300,
        trigger_rule="one_success",
    )
    (
        create_monthly_view()
        >> export_monthly_report()
        >> [
            data_integrity_check(),
            report_exists_on_gdrive_check(),
            report_row_count_check(),
            dbt_checks,
        ]
        >> upload_report()
    )


cgice()
