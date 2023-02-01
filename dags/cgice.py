import pendulum
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.utils.edgemodifier import Label

from workflows.upload_to_google_drive import (
    upload_to_google_drive,
    file_exists_on_google_drive,
)
from workflows.create_bq_external_table import create_external_bq_table

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = "data-warehouse-harbour"

""" 
TODO: Check that the new premium report is automatically uploaded to Google Drive 
on 2023-01-01:

    https://drive.google.com/drive/folders/1DSxToVHiaXdaEoNxT9XoyX_IsF_y4j8u
    
Once verified, modify the folder ID to "1541hzsET3OMSyc4JKlCdl3HmbK9wmXH3"
This corresponds to the GCICE premium report folder on Google Drive.

"""
GOOGLE_DRIVE_FOLDER_ID = "1541hzsET3OMSyc4JKlCdl3HmbK9wmXH3"


@task(
    task_id="update_bq_table",
    outlets=[Dataset("reporting.cgice_premium_bdx_monthly")],
)
def update_bq_table():
    """
    This task creates an external Big Query table on top of the monthly
    cgice_premium_bdx report partitioned by snapshot date.

    The Created Big Query table is:
        ae32-vpcservice-datawarehouse.reporting.cgice_premium_bdx_monthly

    """
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name="reporting",
        table_name="cgice_premium_bdx_monthly",
        source_uri=f"gs://{GCS_BUCKET}/tmp/cgice_premium_bdx_monthly/*",
        partition_uri=f"gs://{GCS_BUCKET}/tmp/cgice_premium_bdx_monthly",
        source_format="CSV",
        schema_path="dags/schemas/reporting/cgice_premium_bdx/bq_schema.json",
        skip_leading_rows=1,
    )


@task.branch(task_id="upload_branch")
def upload_branch(data_interval_start: pendulum.datetime = None):
    """
    Branch and upload the monthly report to Google Drive only if the report is not yet
    on Google Drive.
    """
    start_date = data_interval_start
    filename = f"Napo_Pet_Premium_Bdx_New_{start_date.year}_{start_date.month}.csv"
    if file_exists_on_google_drive(file_name=filename, token_file=OAUTH_TOKEN_FILE):
        return "no_op"

    return "upload_report"


@task(task_id="upload_report")
def upload_report(
    data_interval_start: pendulum.datetime = None,
    data_interval_end: pendulum.datetime = None,
):
    """
    This task uploads a monthly report to a shared Google Drive folder in csv format.
    The reporting period starts from the first of the previous month up to the first
    of the reporting month (exclusive).

    The Google shared drive folder is currently:
        https://drive.google.com/drive/folders/1DSxToVHiaXdaEoNxT9XoyX_IsF_y4j8u

    """
    start_date = data_interval_start
    end_date = data_interval_end
    filename = f"Napo_Pet_Premium_Bdx_New_{start_date.year}_{start_date.month:02d}.csv"
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"tmp/cgice_premium_bdx_monthly/run_date={end_date.date()}/{filename}",
        gdrive_folder_id=GOOGLE_DRIVE_FOLDER_ID,
        gdrive_file_name=f"{filename}",
        token_file=OAUTH_TOKEN_FILE,
    )


@dag(
    dag_id="cgice_premium_bdx",
    start_date=pendulum.datetime(2022, 12, 1, tz="UTC"),
    schedule_interval="0 2 1 * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["reporting", "monthly"],
)
def cgice_premium_bdx():
    """
    The cgice_premium_bdx sales report is currently generated on a reporting instance
    via a Django management command.

    A k8s cronjob is setup on the production cluster to sync monthly reports to GCS at
    01:00 on the first of each month. [0 1 1 * *]

    This Airflow pipeline will automate Big Query table creation and Google Drive
    upload.

    This pipeline is scheduled to run monthly at 02:00 on the first of each month.
    """
    report_stub = EmptyOperator(task_id="report_stub")
    no_op = EmptyOperator(task_id="no_op")

    t1 = update_bq_table()
    t2 = upload_branch()
    t3 = upload_report()

    report_stub >> t1 >> t2 >> t3
    t2 >> Label("File exists") >> no_op


cgice_premium_bdx()
