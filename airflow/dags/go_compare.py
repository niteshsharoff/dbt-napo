import logging
from datetime import timedelta

import pendulum
from dags.workflows.common import gcs_csv_to_dataframe
from dags.workflows.create_bq_view import create_bq_view
from dags.workflows.export_bq_result_to_gcs import export_query_to_gcs
from dags.workflows.reporting.gocompare.utils import (
    get_monthly_report_name,
    get_monthly_reporting_period,
    get_monthly_view_name,
    get_weekly_report_name,
    get_weekly_reporting_period,
    get_weekly_view_name,
)
from dags.workflows.upload_to_google_drive import (
    file_exists_on_google_drive,
    upload_to_google_drive,
)
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

JINJA_ENV = Environment(loader=FileSystemLoader("dags/"))
SFTP_SCRIPT = JINJA_ENV.get_template("bash/sftp_upload.sh")
SFTP_HOST = Variable.get("GOCOMPARE_SFTP_HOST")
SFTP_PORT = Variable.get("GOCOMPARE_SFTP_PORT")
SFTP_USER = Variable.get("GOCOMPARE_SFTP_USER")
SFTP_KEYFILE = Variable.get("GOCOMPARE_SFTP_KEYFILE")

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

BASTION_NAME = "ae32-bastion-host"
BASTION_ZONE = "europe-west2-a"
BASTION_PROJECT = "ae32-vpc-host"

DBT_CLOUD_JOB_ID = 289269

GOOGLE_DRIVE_WEEKLY_FOLDER_ID = "1Q6pq9fCqyZOCOS8aZcHpRjSv-yWdHq3D"
GOOGLE_DRIVE_MONTHLY_FOLDER_ID = "1fW9ZqlhTc5gb7KvxJF8MR38IPKxb_RYO"

PARTITION_INTEGRITY_CHECK = JINJA_ENV.get_template("sql/partition_integrity_check.sql")
GOCOMPARE_SALES_REPORT_QUERY = JINJA_ENV.get_template("sql/gocompare_sales_report.sql")
BQ_DATASET = "reporting"
WEEKLY_TABLE = "gocompare_sales_report_weekly"
GOCOMPARE_MONTHLY_TABLE = "gocompare_sales_report_monthly"
WHITELABEL_MONTHLY_TABLE = "whitelabel_sales_report_monthly"


def sftp_task_wrapper(
    task_id: str,
    local_dir: str,
    remote_dir: str,
    gcs_uri: str,
) -> SSHOperator:
    """
    This Airflow task wrapper returns a SSHOperator which will execute a shell script
    on the bastion host for uploading a report via SFTP.

    :param task_id: Airflow task id
    :param local_dir: Directory the report will be downloaded to on the Bastion Host
    :param remote_dir: Directory the report will be uploaded to on the SFTP server
    :param gcs_uri: URI of report on GCS
    """
    command = SFTP_SCRIPT.render(
        dict(
            local_dir=local_dir,
            remote_dir=remote_dir,
            gcs_uri=gcs_uri,
            host=SFTP_HOST,
            port=SFTP_PORT,
            user=SFTP_USER,
            keyfile=SFTP_KEYFILE,
            use_keyfile=True,
        )
    )
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ComputeEngineSSHHook(
            instance_name=BASTION_NAME,
            zone=BASTION_ZONE,
            project_id=BASTION_PROJECT,
            use_oslogin=False,
            use_iap_tunnel=True,
            use_internal_ip=True,
            expire_time=60,
        ),
        command=command,
    )


@task.branch
def weekly_branch(data_interval_end: pendulum.datetime = None):
    if data_interval_end.day_of_week == 1:
        return "create_weekly_view"

    return "no_op"


@task(outlets=[Dataset(f"{BQ_DATASET}.{WEEKLY_TABLE}_*")])
def create_weekly_view(data_interval_end: pendulum.datetime = None):
    """
    This task creates a Big Query weekly view on top of dbt_marts.gocompare_cumulative_sales_report and dbt_marts.whitelabel_cumulative_sales_report and unions the two together
    """
    start_date, _, run_date = get_weekly_reporting_period(data_interval_end)
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        view_name=get_weekly_view_name(start_date),
        view_query=f"""
            {GOCOMPARE_SALES_REPORT_QUERY.render(
                dict(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=run_date.strftime("%Y-%m-%d"),
                    snapshot_at=run_date.strftime("%Y-%m-%d"),
                    source_table=GOCOMPARE_MONTHLY_TABLE
                )
            )}
            UNION ALL
            {GOCOMPARE_SALES_REPORT_QUERY.render(
                dict(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=run_date.strftime("%Y-%m-%d"),
                    snapshot_at=run_date.strftime("%Y-%m-%d"),
                    source_table=WHITELABEL_MONTHLY_TABLE
                )
            )}
            """,
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


@task
def weekly_report_exists_check(data_interval_end: pendulum.datetime = None):
    """
    This task checks if a weekly report already exists on Google Drive.
    """
    _, end_date, _ = get_weekly_reporting_period(data_interval_end)
    report_name = get_weekly_report_name(end_date)
    if file_exists_on_google_drive(
        file_name=report_name,
        token_file=OAUTH_TOKEN_FILE,
    ):
        raise AirflowSkipException


@task
def weekly_data_integrity_check(data_interval_end: pendulum.datetime = None):
    """
    This task check if the number of partitions present in source tables is the same
    as the number of days in the reporting period.
    """
    start_date, _, run_date = get_monthly_reporting_period(data_interval_end)
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = PARTITION_INTEGRITY_CHECK.render(
        source_tables=["policy", "customer", "pet"],
        start_date=start_date,
        end_date=run_date,
    )
    logging.info("Checking partition integrity with query: \n" + query)
    query_job = client.query(query)
    result = query_job.result()
    if result.total_rows != 0:
        raise AirflowFailException


@task
def weekly_report_row_count_check(data_interval_end: pendulum.datetime = None):
    """
    This task checks if the report is empty.
    """
    _, _, run_date = get_weekly_reporting_period(data_interval_end)
    filename = get_weekly_report_name(run_date)
    df = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{BQ_DATASET}/{WEEKLY_TABLE}/run_date={run_date.date()}",
        pattern=f"*/{filename}",
    )
    df.head()
    if df.empty:
        raise AirflowFailException


@task
def upload_weekly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the weekly report to Google Drive.
    """
    _, _, run_date = get_weekly_reporting_period(data_interval_end)
    report_name = get_weekly_report_name(run_date)
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{WEEKLY_TABLE}/run_date={run_date.date()}/{report_name}",
        gdrive_folder_id=GOOGLE_DRIVE_WEEKLY_FOLDER_ID,
        gdrive_file_name=f"{report_name}",
        token_file=OAUTH_TOKEN_FILE,
    )


@task.branch
def monthly_branch(data_interval_end: pendulum.datetime = None):
    if data_interval_end.day == 1:
        return "create_monthly_view"

    return "no_op"


@task(outlets=[Dataset(f"{BQ_DATASET}.{WEEKLY_TABLE}_*")])
def create_monthly_view(data_interval_end: pendulum.datetime = None):
    """
    This task creates a Big Query monthly view on top of dbt_marts.gocompare_cumulative_sales_report and dbt_marts.whitelabel_cumulative_sales_report
    """
    start_date, _, run_date = get_monthly_reporting_period(data_interval_end)
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        view_name=get_monthly_view_name(start_date, 'gocompare'),
        view_query=GOCOMPARE_SALES_REPORT_QUERY.render(
            dict(
                source_table='dbt_marts.gocompare_cumulative_sales_report',
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=run_date.strftime("%Y-%m-%d"),
                snapshot_at=run_date.strftime("%Y-%m-%d"),
            )
        ),
    )
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        view_name=get_monthly_view_name(start_date, 'stickeewhitelabel'),
        view_query=GOCOMPARE_SALES_REPORT_QUERY.render(
            dict(
                source_table='dbt_marts.whitelabel_cumulative_sales_report',
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=run_date.strftime("%Y-%m-%d"),
                snapshot_at=run_date.strftime("%Y-%m-%d"),
            )
        ),
    )


@task
def export_monthly_report(data_interval_end: pendulum.datetime = None):
    """
    This task exports the query results of a monthly view to Cloud Storage.
    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    gcs_file_name = get_monthly_report_name(run_date)
    # export gocompare monthly report
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{f"{GOCOMPARE_MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}"}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/{}/run_date={}/{}".format(
            BQ_DATASET,
            GOCOMPARE_MONTHLY_TABLE,
            run_date.date(),
            gcs_file_name,
        ),
    )
    # export whitelabel monthly report
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{f"{WHITELABEL_MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}"}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/{}/run_date={}/{}".format(
            BQ_DATASET,
            WHITELABEL_MONTHLY_TABLE,
            run_date.date(),
            gcs_file_name,
        ),
    )


@task
def monthly_report_exists_check(data_interval_end: pendulum.datetime = None):
    """
    This task checks if the report already exists on Google Drive
    """
    # check if gocompare report exists
    _, end_date, _ = get_monthly_reporting_period(data_interval_end)
    # check if gocompare report exists
    report_name = get_monthly_report_name(end_date, source='gocompare')
    if file_exists_on_google_drive(
        file_name=report_name,
        token_file=OAUTH_TOKEN_FILE,
    ):
        raise AirflowSkipException
    # check if whitelabel report exists
    report_name = get_monthly_report_name(end_date, source='stickeewhitelabel')
    if file_exists_on_google_drive(
        file_name=report_name,
        token_file=OAUTH_TOKEN_FILE,
    ):
        raise AirflowSkipException

@task
def monthly_data_integrity_check(data_interval_end: pendulum.datetime = None):
    """
    This task check if the number of partitions present in source tables is the same
    as the number of days in the reporting period.
    """
    start_date, _, run_date = get_monthly_reporting_period(data_interval_end)
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = PARTITION_INTEGRITY_CHECK.render(
        source_tables=["policy", "customer", "pet"],
        start_date=start_date,
        end_date=run_date,
    )
    logging.info("Checking partition integrity with query: \n" + query)
    query_job = client.query(query)
    result = query_job.result()
    if result.total_rows != 0:
        raise AirflowFailException


@task
def monthly_report_row_count_check(data_interval_end: pendulum.datetime = None):
    """
    This task checks if the report is empty.
    """
    _, end_date, run_date = get_monthly_reporting_period(data_interval_end)
    # check gocompare
    filename = get_monthly_report_name(end_date, source='gocompare')
    df = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{BQ_DATASET}/{GOCOMPARE_MONTHLY_TABLE}/run_date={run_date.date()}",
        pattern=f"*/{filename}",
    )
    df.head()
    if df.empty:
        raise AirflowFailException

    # check whitelabel
    filename = get_monthly_report_name(end_date, source='stickeewhitelabel')
    df = gcs_csv_to_dataframe(
        gcs_bucket=GCS_BUCKET,
        gcs_folder=f"{BQ_DATASET}/{WHITELABEL_MONTHLY_TABLE}/run_date={run_date.date()}",
        pattern=f"*/{filename}",
    )
    df.head()
    if df.empty:
        raise AirflowFailException


@task
def upload_monthly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the monthly report to Google Drive.
    """

    _, end_date, run_date = get_monthly_reporting_period(data_interval_end)
    # gocompare
    report_name = get_monthly_report_name(end_date, source='gocompare')
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{GOCOMPARE_MONTHLY_TABLE}/run_date={run_date.date()}/{report_name}",
        gdrive_folder_id=GOOGLE_DRIVE_MONTHLY_FOLDER_ID,
        gdrive_file_name=f"{report_name}",
        token_file=OAUTH_TOKEN_FILE,
    )
    # whitelabel
    report_name = get_monthly_report_name(end_date, source='stickeewhitelabel')
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{WHITELABEL_MONTHLY_TABLE}/run_date={run_date.date()}/{report_name}",
        gdrive_folder_id=GOOGLE_DRIVE_MONTHLY_FOLDER_ID,
        gdrive_file_name=f"{report_name}",
        token_file=OAUTH_TOKEN_FILE,
    )


@dag(
    dag_id="go_compare",
    start_date=pendulum.datetime(2023, 7, 23, tz="UTC"),
    schedule_interval="15 4 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["reporting", "weekly", "monthly"],
)
def go_compare():
    """
    The GoCompare daily sales report is generated on a reporting instance via a Django
    management command.

    This Airflow pipeline will automate report generation and delivery to SFTP servers
    and Google Drive.

    This pipeline is scheduled to run daily at 04:15 but weekly and monthly tasks will
    be skipped unless it's the first day of the week or month.
    """
    run_date = "{{ data_interval_end.date() }}"
    no_op = EmptyOperator(task_id="no_op", trigger_rule="one_success")

    is_first_day_of_week = weekly_branch()
    is_first_day_of_month = monthly_branch()

    dbt_checks = ExternalTaskSensor(
        task_id="dbt_checks",
        # task_id in dags/dbt.py
        external_dag_id="dbt",
        external_task_id="dbt_test",
        # dbt cloud tests should not take longer than 5 minutes
        timeout=300,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        # 15 1 * * *
        execution_delta=timedelta(hours=3),
        trigger_rule="one_success",
    )

    # Weekly tasks branch
    with TaskGroup(group_id="first_of_week", prefix_group_id=False) as weekly_tasks:
        sftp_weekly_report = sftp_task_wrapper(
            task_id="sftp_weekly_report",
            local_dir="gocompare/weekly",
            remote_dir="incoming",
            gcs_uri="gs://{}/{}/{}/run_date={}/*".format(
                GCS_BUCKET,
                BQ_DATASET,
                WEEKLY_TABLE,
                run_date,
            ),
        )
        (
            create_weekly_view()
            >> export_weekly_report()
            >> [
                dbt_checks,
                weekly_report_exists_check(),
                weekly_data_integrity_check(),
                weekly_report_row_count_check(),
            ]
            >> sftp_weekly_report
            >> upload_weekly_report()
        )

    # Monthly tasks branch
    with TaskGroup(group_id="first_of_month", prefix_group_id=False) as monthly_tasks:
        # gocompare
        sftp_gocompare_monthly_report = sftp_task_wrapper(
            task_id="sftp_monthly_report",
            local_dir="gocompare/monthly",
            remote_dir="incoming",
            gcs_uri="gs://{}/{}/{}/run_date={}/*".format(
                GCS_BUCKET,
                BQ_DATASET,
                GOCOMPARE_MONTHLY_TABLE,
                run_date,
            ),
        )
        # whitelabel
        sftp_whitelabel_monthly_report = sftp_task_wrapper(
            task_id="sftp_monthly_report",
            local_dir="whitelabel/monthly",
            remote_dir="incoming",
            gcs_uri="gs://{}/{}/{}/run_date={}/*".format(
                GCS_BUCKET,
                BQ_DATASET,
                WHITELABEL_MONTHLY_TABLE,
                run_date,
            ),
        )
        (
            create_monthly_view()
            >> export_monthly_report()
            >> [
                dbt_checks,
                monthly_report_exists_check(),
                monthly_data_integrity_check(),
                monthly_report_row_count_check(),
            ]
            >> sftp_gocompare_monthly_report
            >> sftp_whitelabel_monthly_report
            >> upload_monthly_report()
        )

    is_first_day_of_week >> weekly_tasks
    is_first_day_of_week >> no_op

    is_first_day_of_month >> monthly_tasks
    is_first_day_of_month >> no_op


go_compare()
