import pendulum
from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.python import task
from airflow.operators.empty import EmptyOperator

from dags.workflows.create_bq_view import create_quotezone_sales_view
from workflows.upload_to_google_drive import (
    upload_to_google_drive,
)
from workflows.export_bq_result_to_gcs import export_query_to_gcs


OAUTH_TOKEN_FILE = Variable.get("OAUTH_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
GOOGLE_DRIVE_MONTHLY_FOLDER_ID = "1RQVpn-UMWsEaqF4yIA0R2WXi5QM9nBFs"
BQ_DATASET = "reporting"

MONTHLY_TABLE = "quotezone_sales_report_monthly"


@task(task_id="create_view")
def create_view(data_interval_end: pendulum.datetime = None):
    """
    This task creates / updates an external Big Query table on top of the generated
    monthly quotezon sales report partitioned by run date.

    The Created Big Query table is:
        ae32-vpcservice-datawarehouse.reporting.quotezone_sales_report_monthly_{YYYYMMDD}
    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    create_quotezone_sales_view(
        project_name=GCP_PROJECT_ID,
        dataset_name="reporting",
        view_name=f"quotezone_sales_report_monthly_{start_date.format('YYYYMMDD')}",
        start_date=start_date,
        end_date=run_date,
        snapshot_at=run_date,
    )


@task(task_id="export_report")
def export_report(data_interval_end: pendulum.datetime = None):
    """
    This task query's the monthly view table, stores the results in a temp table and
    exports the result to Cloud Storage.

    The monthly view table is:
        ae32-vpcservice-datawarehouse.reporting.quotezone_sales_report_monthly_{YYYYMMDD}

    The results are stored in a tmp table which expires after 1 hour:
        ae32-vpcservice-datawarehouse.reporting.tmp

    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    start_date = pendulum.datetime(start_date.year, start_date.month, 1, tz="UTC")
    end_date = pendulum.datetime(run_date.year, run_date.month, 1, tz="UTC")
    table_name = f"{MONTHLY_TABLE}_{start_date.format('YYYYMMDD')}"
    gcs_file_name = "Quotezone_{start_date}_{end_date}.csv".format(
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


@task(task_id="upload_report", trigger_rule="none_failed_or_skipped")
def upload_report(data_interval_end: pendulum.datetime = None):
    """
    This task uploads the monthly report to a shared Google Drive folder in csv format.
    The report month is the same as the ETL run_date but the data should be from the
    month prior.

    The Google shared drive folder is:
        https://drive.google.com/drive/folders/1RQVpn-UMWsEaqF4yIA0R2WXi5QM9nBFs

    """
    run_date = data_interval_end
    start_date = run_date.subtract(months=1)
    start_date = pendulum.datetime(start_date.year, start_date.month, 1, tz="UTC")
    end_date = pendulum.datetime(run_date.year, run_date.month, 1, tz="UTC")
    gcs_file_name = "Quotezone_{start_date}_{end_date}.csv".format(
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
    dag_id="quotezone",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="0 0 1 * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    max_active_tasks=7,
    tags=["reporting", "monthly"],
)
def quotezone():
    """
    This Airflow pipeline will automate Big Query table creation, Google Drive upload
    and SFTP upload.

    This pipeline is scheduled to run daily at the first day of the month.
    """
    placeholder = EmptyOperator(task_id="generate_report")
    placeholder >> create_view() >> export_report() >> upload_report()

quotezone()