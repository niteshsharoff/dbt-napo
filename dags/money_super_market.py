import pendulum
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from jinja2 import Environment, FileSystemLoader

from dags.workflows.create_bq_external_table import create_external_bq_table
from dags.workflows.create_bq_view import create_msm_sales_view
from dags.workflows.export_bq_result_to_gcs import export_query_to_gcs
from dags.workflows.upload_to_google_drive import file_exists_on_google_drive, \
    upload_to_google_drive

JINJA_ENV = Environment(loader=FileSystemLoader("dags/bash/"))
SFTP_SCRIPT = JINJA_ENV.get_template("sftp_upload.sh")
SFTP_HOST = Variable.get("MSM_SFTP_HOST")
SFTP_PORT = Variable.get("MSM_SFTP_PORT")
SFTP_USER = Variable.get("MSM_SFTP_USER")
SFTP_PASS = Variable.get("MSM_SFTP_PASSWORD")

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

BASTION_NAME = "ae32-bastion-host"
BASTION_ZONE = "europe-west2-a"
BASTION_PROJECT = "ae32-vpc-host"
GOOGLE_DRIVE_MONTHLY_FOLDER_ID = "14KmTbfcFH17UJyZ90KWBAMtMs6WRWDUN"
GOOGLE_DRIVE_WEEKLY_FOLDER_ID = "1jpWcRUismIBpbDh3_DH0aUCPOWZaiNow"
BQ_DATASET = "reporting"
DAILY_TABLE = "msm_sales_report_daily"
WEEKLY_TABLE = "msm_sales_report_weekly"
MONTHLY_TABLE = "msm_sales_report_monthly"


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
    outlets=[Dataset(f"reporting.{DAILY_TABLE}")],
)
def create_daily_table():
    """
    This task creates an external Big Query table using daily msm sales report
    partitioned by run date.
    """
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=BQ_DATASET,
        table_name=DAILY_TABLE,
        source_uri=f"gs://{GCS_BUCKET}/tmp/msm/*",
        partition_uri=f"gs://{GCS_BUCKET}/tmp/msm",
        source_format="CSV",
        schema_path="dags/schemas/reporting/msm_sales_report/bq_schema.json",
        skip_leading_rows=1,
    )


@task.branch(task_id="weekly_branch")
def weekly_branch(data_interval_end: pendulum.datetime = None):
    if data_interval_end.day_of_week == 1:
        return "create_weekly_view"

    return "no_op"


@task(
    task_id="create_weekly_view",
    outlets=[Dataset(f"reporting.{WEEKLY_TABLE}_*")],
)
def create_weekly_view(data_interval_end: pendulum.datetime = None):
    """
    This task creates a Big Query weekly view on top of the msm_sales_report_daily
    table. The view name is derived from the first day of the reporting period.
    """
    run_date = data_interval_end
    end_date = run_date.subtract(days=1)
    start_date = pendulum.parse(f"{end_date.year}W{end_date.week_of_year}")
    create_msm_sales_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=DAILY_TABLE,
        view_name=f"{WEEKLY_TABLE}_{start_date.format('YYYYMMDD')}",
        start_date=start_date,
        end_date=run_date,
    )


@task(task_id="export_weekly_report")
def export_weekly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the query results of a weekly view to Cloud Storage.
    """
    run_date = data_interval_end
    end_date = run_date.subtract(days=1)
    start_date = pendulum.parse(f"{end_date.year}W{end_date.week_of_year}")
    view_name = f"{WEEKLY_TABLE}_{start_date.format('YYYYMMDD')}"
    gcs_file_name = "Napo_Pet_Weekly_{}.csv".format(end_date.format("YYYYMMDD"))
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


@task.branch(task_id="upload_weekly_branch")
def upload_weekly_branch(data_interval_end: pendulum.datetime = None):
    """
    This task checks if a weekly report already exists on Google Drive.
    """
    run_date = data_interval_end
    end_date = run_date.subtract(days=1)
    report_name = "Napo_Pet_Weekly_{}.csv".format(end_date.format("YYYYMMDD"))
    if file_exists_on_google_drive(file_name=report_name, token_file=OAUTH_TOKEN_FILE):
        return "no_op"

    return "sftp_weekly_report"


@task(task_id="upload_weekly_report")
def upload_weekly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the weekly report to Google Drive.
    """
    run_date = data_interval_end
    end_date = run_date.subtract(days=1)
    report_name = "Napo_Pet_Weekly_{}.csv".format(end_date.format("YYYYMMDD"))
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{WEEKLY_TABLE}/run_date={run_date.date()}/{report_name}",
        gdrive_folder_id=GOOGLE_DRIVE_WEEKLY_FOLDER_ID,
        gdrive_file_name=f"{report_name}",
        token_file=OAUTH_TOKEN_FILE,
    )


@task.branch(task_id="monthly_branch")
def monthly_branch(data_interval_end: pendulum.datetime = None):
    if data_interval_end.day == 1:
        return "create_monthly_view"

    return "no_op"


@task(
    task_id="create_monthly_view",
    outlets=[Dataset(f"reporting.{MONTHLY_TABLE}_*")],
)
def create_monthly_view(data_interval_end: pendulum.datetime = None):
    """
    This task creates a Big Query monthly view on top of the msm_sales_report_daily
    table. The view name is derived from the first day of the reporting period.
    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    create_msm_sales_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=DAILY_TABLE,
        view_name=f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}",
        start_date=start_date,
        end_date=run_date,
    )


@task(task_id="export_monthly_report")
def export_monthly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the query results of a monthly view to Cloud Storage.
    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    end_date = run_date.subtract(days=1)
    view_name = f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}"
    gcs_file_name = "Napo_Pet_Monthly_{}.csv".format(end_date.format("YYYYMMDD"))
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


@task.branch(task_id="upload_monthly_branch")
def upload_monthly_branch(data_interval_end: pendulum.datetime = None):
    run_date = data_interval_end
    end_date = run_date.subtract(days=1)
    report_name = "Napo_Pet_Monthly_{}.csv".format(end_date.format("YYYYMMDD"))
    if file_exists_on_google_drive(file_name=report_name, token_file=OAUTH_TOKEN_FILE):
        return "no_op"

    return "sftp_monthly_report"


@task(task_id="upload_monthly_report")
def upload_monthly_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the monthly report to Google Drive.
    """
    run_date = data_interval_end
    end_date = run_date.subtract(days=1)
    report_name = "Napo_Pet_Monthly_{}.csv".format(end_date.format("YYYYMMDD"))
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{MONTHLY_TABLE}/run_date={run_date.date()}/{report_name}",
        gdrive_folder_id=GOOGLE_DRIVE_MONTHLY_FOLDER_ID,
        gdrive_file_name=f"{report_name}",
        token_file=OAUTH_TOKEN_FILE,
    )



@dag(
    dag_id="money_super_market",
    start_date=pendulum.datetime(2022, 11, 30, tz="UTC"),
    schedule_interval="0 2 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["reporting", "weekly", "monthly"],
)
def money_super_market():
    """
    The MoneySuperMarket sales report is generated on a reporting instance via a Django
    management command.

    A k8s cronjob is setup on the production cluster to sync the reports to GCS daily
    at 01:00.

    This Airflow pipeline will automate Big Query tables creation, Google Drive upload
    and SFTP upload.

    This pipeline is scheduled to run daily at 02:00 but weekly and monthly tasks will
    be skipped unless it's the first day of the week or month.
    """
    run_date = "{{ data_interval_end.date() }}"
    report_stub = EmptyOperator(task_id="report_stub")
    no_op = EmptyOperator(task_id="no_op", trigger_rule="one_success")

    is_first_day_of_week = weekly_branch()
    is_first_day_of_month = monthly_branch()

    with TaskGroup(group_id="first_of_week", prefix_group_id=False) as weekly_tasks:
        report_exists_branch = upload_weekly_branch()
        create_weekly_view() >> export_weekly_report() >> report_exists_branch
        sftp_weekly_report = sftp_task_wrapper(
            task_id="sftp_weekly_report",
            local_dir="msm/weekly",
            remote_dir="Weekly",
            gcs_uri="gs://{}/{}/msm_sales_report_weekly/run_date={}/*".format(
                GCS_BUCKET,
                BQ_DATASET,
                run_date,
            ),
        )
        report_exists_branch >> sftp_weekly_report >> upload_weekly_report()
        report_exists_branch >> Label("File exists") >> no_op

    with TaskGroup(group_id="first_of_month", prefix_group_id=False) as monthly_tasks:
        report_exists_branch = upload_monthly_branch()
        create_monthly_view() >> export_monthly_report() >> report_exists_branch
        sftp_monthly_report = sftp_task_wrapper(
            task_id="sftp_monthly_report",
            local_dir="msm/monthly",
            remote_dir="Monthly",
            gcs_uri="gs://{}/{}/msm_sales_report_monthly/run_date={}/*".format(
                GCS_BUCKET,
                BQ_DATASET,
                run_date,
            ),
        )
        report_exists_branch >> sftp_monthly_report >> upload_monthly_report()
        report_exists_branch >> Label("File exists") >> no_op

    # DAG structure
    report_stub >> create_daily_table() >> [is_first_day_of_week, is_first_day_of_month]

    is_first_day_of_week >> weekly_tasks,
    is_first_day_of_week >> no_op

    is_first_day_of_month >> monthly_tasks,
    is_first_day_of_month >> no_op


money_super_market()
