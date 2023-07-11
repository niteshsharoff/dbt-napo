import pendulum
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from jinja2 import Environment, FileSystemLoader

from workflows.export_bq_result_to_gcs import export_query_to_gcs
from workflows.upload_to_google_drive import (
    upload_to_google_drive,
    file_exists_on_google_drive,
)
from workflows.create_bq_external_table import create_external_bq_table
from workflows.create_bq_view import create_ctm_sales_monthly_view

JINJA_ENV = Environment(loader=FileSystemLoader("dags/bash/"))
SFTP_SCRIPT = JINJA_ENV.get_template("sftp_upload.sh")
SFTP_HOST = Variable.get("CTM_SFTP_HOST")
SFTP_PORT = Variable.get("CTM_SFTP_PORT")
SFTP_USER = Variable.get("CTM_SFTP_USER")
SFTP_PASS = Variable.get("CTM_SFTP_PASSWORD")

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

BASTION_NAME = "ae32-bastion-host"
BASTION_ZONE = "europe-west2-a"
BASTION_PROJECT = "ae32-vpc-host"
GOOGLE_DRIVE_DAILY_FOLDER_ID = "1JEtPgxP38MWYLaxgZRNwJLkziYTRtRHf"
GOOGLE_DRIVE_MONTHLY_FOLDER_ID = "1iK37ZMa_9dDxkdxgVD9KSNInEsmzRTaR"
BQ_DATASET = "reporting"
DAILY_TABLE = "ctm_sales_report_daily"
MONTHLY_TABLE = "ctm_sales_report_monthly"
TMP_TABLE = "tmp"


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
            password=SFTP_PASS,
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
            expire_time="60m",
        ),
        command=command,
    )


@task(
    task_id="create_daily_table",
    outlets=[Dataset("reporting.ctm_sales_report_daily")],
)
def update_daily_table():
    """
    This task creates / updates an external Big Query table on top of the generated
    daily ctm sales report partitioned by run date. The sales report is currently
    generated by policy service on a reporting instance.

    The Created Big Query table is:
        ae32-vpcservice-datawarehouse.reporting.ctm_sales_report_daily

    """
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=BQ_DATASET,
        table_name=DAILY_TABLE,
        source_uri=f"gs://{GCS_BUCKET}/tmp/ctm/*",
        partition_uri=f"gs://{GCS_BUCKET}/tmp/ctm",
        source_format="CSV",
        schema_path=f"dags/schemas/reporting/ctm_sales_report/bq_schema.json",
        skip_leading_rows=1,
    )


@task.branch(task_id="sftp_daily_check")
def daily_report_exists(data_interval_end: pendulum.datetime = None):
    """
    Branch and upload the daily report to SFTP server only if the report doesn't exist
    yet on Google Drive.

    This is to prevent us from re-uploading historical reports to CTM when running the
    pipeline in backfill mode.
    """
    run_date = data_interval_end
    report_name = f"{DAILY_TABLE}_{run_date.format('YYYYMMDD')}.csv"
    if file_exists_on_google_drive(file_name=report_name, token_file=OAUTH_TOKEN_FILE):
        return "upload_daily_report"

    return "sftp_daily_report"


@task(task_id="upload_daily_report", trigger_rule="none_failed")
def upload_daily_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads a daily report to a shared Google Drive folder in csv format.
    The report day is the same as the ETL run_date but the data should be from the
    day prior.

    The Google shared drive folder is:
        https://drive.google.com/drive/folders/1JEtPgxP38MWYLaxgZRNwJLkziYTRtRHf

    """
    run_date = data_interval_end
    report_date = run_date.subtract(days=1)
    gcs_file_name = "100161_Pet_{0}_{0}_1_2.csv".format(report_date.format("DDMMYYYY"))
    gdrive_file_name = f"{DAILY_TABLE}_{run_date.format('YYYYMMDD')}.csv"
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"tmp/ctm/run_date={run_date.date()}/{gcs_file_name}",
        gdrive_folder_id=GOOGLE_DRIVE_DAILY_FOLDER_ID,
        gdrive_file_name=gdrive_file_name,
        token_file=OAUTH_TOKEN_FILE,
    )


@task.branch(task_id="monthly_branch")
def is_first_of_month(data_interval_end: pendulum.datetime = None):
    """
    Branch and run monthly tasks only on the first day of each month.
    Daily runs not on the first of the month should branch to no op.
    """
    run_date = data_interval_end.date()
    if run_date.day == 1:
        return "create_monthly_view"

    return "no_op"


@task(
    task_id="create_monthly_view",
    outlets=[Dataset("reporting.ctm_sales_report_monthly_*")],
)
def update_monthly_view(data_interval_end: pendulum.datetime = None):
    """
    This task creates a Big Query monthly view on top of the ctm_sales_report_daily
    table.

    The monthly views are named in the following format:
        ae32-vpcservice-datawarehouse.reporting.ctm_sales_report_monthly_{YYYYMMDD}

    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    start_date = pendulum.datetime(start_date.year, start_date.month, 1, tz="UTC")
    end_date = pendulum.datetime(run_date.year, run_date.month, 1, tz="UTC")
    create_ctm_sales_monthly_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=DAILY_TABLE,
        view_name=f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}",
        start_date=start_date,
        end_date=end_date,
    )


@task(task_id="export_monthly_report")
def export_monthly_view(data_interval_end: pendulum.datetime = None):
    """
    This task query's the monthly view table, stores the results in a temp table and
    exports the result to Cloud Storage.

    The monthly view table is:
        ae32-vpcservice-datawarehouse.reporting.ctm_sales_report_monthly_{YYYYMMDD}

    The results are stored in a tmp table which expires after 1 hour:
        ae32-vpcservice-datawarehouse.reporting.tmp

    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    start_date = pendulum.datetime(start_date.year, start_date.month, 1, tz="UTC")
    end_date = pendulum.datetime(run_date.year, run_date.month, 1, tz="UTC")
    table_name = f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}"
    # This is the filename format requested by CTM
    gcs_file_name = "100161_Pet_{start_date}_{end_date}_1_2.csv".format(
        start_date=start_date.format("DDMMYYYY"),
        end_date=end_date.subtract(days=1).format("DDMMYYYY"),
    )
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/{}/run_date={}/{}".format(
            BQ_DATASET,
            MONTHLY_TABLE,
            run_date.date(),
            gcs_file_name,
        ),
    )


@task.branch(task_id="sftp_monthly_check")
def monthly_report_exists(data_interval_end: pendulum.datetime = None):
    """
    Branch and upload the monthly report to SFTP server only if the report doesn't exist
    yet on Google Drive.

    This is to prevent us from re-uploading historical reports to CTM when running the
    pipeline in backfill mode.
    """
    run_date = data_interval_end
    report_name = f"{MONTHLY_TABLE}_{run_date.format('YYYYMMDD')}.csv"
    if file_exists_on_google_drive(file_name=report_name, token_file=OAUTH_TOKEN_FILE):
        return "upload_monthly_report"

    return "sftp_monthly_report"


@task(task_id="upload_monthly_report", trigger_rule="none_failed_or_skipped")
def upload_monthly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the monthly report to a shared Google Drive folder in csv format.
    The report month is the same as the ETL run_date but the data should be from the
    month prior.

    The Google shared drive folder is:
        https://drive.google.com/drive/folders/1iK37ZMa_9dDxkdxgVD9KSNInEsmzRTaR

    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    start_date = pendulum.datetime(start_date.year, start_date.month, 1, tz="UTC")
    end_date = pendulum.datetime(run_date.year, run_date.month, 1, tz="UTC")
    gcs_file_name = "100161_Pet_{start_date}_{end_date}_1_2.csv".format(
        start_date=start_date.format("DDMMYYYY"),
        end_date=end_date.subtract(days=1).format("DDMMYYYY"),
    )
    file_name = f"{MONTHLY_TABLE}_{end_date.format('YYYYMMDD')}.csv"
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{MONTHLY_TABLE}/run_date={run_date.date()}/{gcs_file_name}",
        gdrive_folder_id=GOOGLE_DRIVE_MONTHLY_FOLDER_ID,
        gdrive_file_name=f"{file_name}",
        token_file=OAUTH_TOKEN_FILE,
    )


@dag(
    dag_id="compare_the_market",
    start_date=pendulum.datetime(2022, 12, 31, tz="UTC"),
    # cronjob.batch/ctm-report-daily-upload is currently scheduled at 0 1 * * *
    schedule_interval="0 2 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    max_active_tasks=7,
    tags=["reporting", "daily", "monthly"],
)
def compare_the_market():
    """
    The CompareTheMarket sales report is currently generated on a reporting instance
    via a Django management command.

    A k8s cronjob is setup on the production cluster to sync the reports to GCS.

    This Airflow pipeline will automate Big Query table creation, Google Drive upload
    and SFTP upload.

    This pipeline is scheduled to run daily at 02:00 but monthly tasks will be skipped
    unless it's the first day of the month.
    """
    run_date = "{{ data_interval_end.date() }}"
    placeholder = EmptyOperator(task_id="generate_report")

    # Daily tasks
    daily_table = update_daily_table()
    daily_sftp_check = daily_report_exists()
    daily_sftp = sftp_task_wrapper(
        task_id="sftp_daily_report",
        local_dir="ctm/daily",
        remote_dir="Daily",
        gcs_uri=f"gs://{GCS_BUCKET}/tmp/ctm/run_date={run_date}/*",
    )
    daily_upload = upload_daily_report()

    # Monthly tasks
    no_op = EmptyOperator(task_id="no_op")
    monthly_branch = is_first_of_month()
    monthly_view = update_monthly_view()
    monthly_sftp_check = monthly_report_exists()
    monthly_sftp = sftp_task_wrapper(
        task_id="sftp_monthly_report",
        local_dir="ctm/monthly",
        remote_dir="Monthly",
        gcs_uri=f"gs://{GCS_BUCKET}/reporting/{MONTHLY_TABLE}/run_date={run_date}/*",
    )
    monthly_export = export_monthly_view()
    monthly_upload = upload_monthly_report()

    # DAG
    placeholder >> daily_table >> monthly_branch
    daily_table >> daily_sftp_check
    daily_sftp_check >> daily_sftp >> daily_upload
    daily_sftp_check >> daily_upload

    monthly_branch >> no_op
    monthly_branch >> monthly_view >> monthly_export >> monthly_sftp_check
    monthly_sftp_check >> monthly_sftp >> monthly_upload
    monthly_sftp_check >> monthly_upload


compare_the_market()
