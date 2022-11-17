from datetime import datetime
from datetime import timezone

from airflow import DAG, macros
from airflow.decorators import task, task_group
from airflow.models import Variable

from workflows.export_bq_result_to_gcs import export_table_to_gcs
from workflows.upload_to_google_drive import upload_to_google_drive
from workflows.create_bq_external_table import create_external_bq_table
from workflows.create_bq_view import create_napo_benefit_weekly_view
from workflows.reporting.napo_benefit_code_daily import (
    generate_daily_napo_benefit_code_report,
)

OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# folder_id for 'NapoBenefitCode' folder in Google Drive
GOOGLE_DRIVE_FOLDER_ID = "12CconVtfMqthK8k9Ap9tlGDNyee5JcAy"
BQ_DATASET = "reporting"
DAILY_TABLE = "napo_benefit_code_daily"
WEEKLY_TABLE = "napo_benefit_code_weekly"
TMP_TABLE = "tmp"


@task(task_id="generate_daily_report")
def generate_report(ds=None):
    """
    This task generates the daily napo benefit report which shows who qualifies on each
    run date.

    Daily report runs are uploaded to:
        gs://data-warehouse-harbour/reporting/napo_benefit_code_daily/run_date=*
    """
    run_date = macros.datetime.strptime(ds, "%Y-%m-%d")
    run_date.replace(tzinfo=timezone.utc)
    run_date = run_date.replace(tzinfo=timezone.utc)
    generate_daily_napo_benefit_code_report(
        run_date=run_date,
        project_id=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_prefix=f"{BQ_DATASET}/{DAILY_TABLE}",
    )


@task(task_id="create_daily_table")
def create_table():
    """
    This task creates an External Big Query table on top of the generated daily
    napo_benefit_code report partitioned by run date.

    The Created Big Query table is:
        ae32-vpcservice-datawarehouse.reporting.napo_benefit_code_daily

    The table's data source is:
        gs://data-warehouse-harbour/reporting/napo_benefit_code_daily/*
    """
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=BQ_DATASET,
        table_name=DAILY_TABLE,
        source_uri=f"gs://{GCS_BUCKET}/{BQ_DATASET}/{DAILY_TABLE}/*",
        partition_uri=f"gs://{GCS_BUCKET}/{BQ_DATASET}/{DAILY_TABLE}",
        source_format="CSV",
        schema_path=f"dags/schemas/csv/{DAILY_TABLE}/1.0.0/bq_schema.json",
        skip_leading_rows=0,
    )


@task(task_id="update_weekly_view")
def create_view(ds=None):
    """
    This task creates a Big Query weekly view on top of the napo_benefit_code_daily
    table.

    The weekly views are named in the following format:
        ae32-vpcservice-datawarehouse.reporting.napo_benefit_code_weekly_{YYYYmmdd}

    The underlying query for the view is:
        select *
        from `ae32-vpcservice-datawarehouse.reporting.napo_benefit_code_daily`
        where extract(year from run_date) = {YYYY}
        and extract(week from run_date) = {WW}
    """
    run_date = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    run_week = run_date.strftime("%Y-W%V-1")
    run_week = datetime.strptime(run_week, "%G-W%V-%u")
    create_napo_benefit_weekly_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=DAILY_TABLE,
        view_name=f"{WEEKLY_TABLE}_{run_week.strftime('%Y%m%d')}",
        run_date=run_date,
    )


@task(task_id="export_weekly_report")
def export_view_to_gcs(ds=None):
    """
    This task query's the weekly view table, stores the results in a temp table and
    exports the result to Cloud Storage.

    The weekly view table is:
        ae32-vpcservice-datawarehouse.reporting.napo_benefit_code_weekly_{YYYYmmdd}

    The results are stored in a tmp table which expires after 1 hour:
        ae32-vpcservice-datawarehouse.reporting.tmp

    The destination Cloud Storage uri is:
        gs://data-warehouse-harbour/reporting/napo_benefit_code_weekly/*
    """
    run_date = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    run_week = run_date.strftime("%Y-W%V-1")
    run_week = datetime.strptime(run_week, "%G-W%V-%u")
    table_name = f"{WEEKLY_TABLE}_{run_week.strftime('%Y%m%d')}"
    export_table_to_gcs(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        src_table=table_name,
        tmp_table=TMP_TABLE,
        gcs_uri=f"gs://{GCS_BUCKET}/{BQ_DATASET}/{WEEKLY_TABLE}/{table_name}.csv",
    )


@task(task_id="upload_csv_to_google_drive")
def upload_csv_to_google_drive(ds=None):
    """
    This task uploads a weekly report to a shared Google Drive folder in csv format.

    The Google shared drive folder is:
        https://drive.google.com/drive/folders/12CconVtfMqthK8k9Ap9tlGDNyee5JcAy

    The CSV reports are stored at:
        gs://data-warehouse-harbour/reporting/napo_benefit_code_weekly/*
    """
    run_date = macros.datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    run_week = run_date.strftime("%Y-W%V-1")
    run_week = datetime.strptime(run_week, "%G-W%V-%u")
    file_name = f"{WEEKLY_TABLE}_{run_week.strftime('%Y%m%d')}.csv"
    upload_to_google_drive(
        project_name=GCP_PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
        gcs_path=f"{BQ_DATASET}/{WEEKLY_TABLE}/{file_name}",
        gdrive_folder_id=GOOGLE_DRIVE_FOLDER_ID,
        gdrive_file_name=f"{file_name}",
        token_file=OAUTH_TOKEN_FILE,
    )


with DAG(
    dag_id="napo_benefit_code",
    start_date=datetime(2022, 8, 23),  # The beginning of time for Napo Benefit Codes
    schedule_interval="0 1 * * *",
    default_args={"retries": 0},
    max_active_runs=1,  # Setting this to 1 to avoid concurrency issues with Google Drive
    catchup=True,
) as dag:

    @task_group(group_id="big_query")
    def create_table_and_view():
        create_table() >> create_view() >> export_view_to_gcs()

    generate_report() >> create_table_and_view() >> upload_csv_to_google_drive()
