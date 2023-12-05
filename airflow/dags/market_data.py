import logging
import os

import boto3
import pendulum
from botocore.exceptions import ClientError
from google.cloud import storage

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.dag import dag

from workflows.create_bq_external_table import create_external_bq_table

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
AWS_ACCESS_KEY = Variable.get("MARKET_DATA_AWS_ACCESS_KEY")
AWS_SECRET_KEY = Variable.get("MARKET_DATA_AWS_SECRET_KEY")

# List S3 buckets to find
AWS_BUCKET_NAME = "petbucket2023"
GCS_BUCKET_NAME = "data-warehouse-harbour"


@task
def load_csv_from_s3_to_gcs(
    data_interval_start: pendulum.datetime = None,
    data_interval_end: pendulum.datetime = None,
):
    # Expect 1 day lag. The run date is today, but the file we load is generated yesterday
    run_date = data_interval_end.date()

    # Filenames stored externally in the format 'pet_quotes_2023-11-22_00:00:00:000_data.csv'
    filename = f"pet_quotes_{data_interval_start.date()}_00:00:00:000_data.csv"
    s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        s3_client.download_file(AWS_BUCKET_NAME, filename, filename)
    except ClientError as e:
        logging.warn(f"Failed to download file: {e}")
        raise AirflowSkipException

    # Upload to gs://data-warehouse-harbour/raw/market_data/*
    gcs_path = f"raw/market_data/run_date={run_date}/{filename}"
    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    gcs_bucket = gcs_client.get_bucket(GCS_BUCKET_NAME)
    blob = gcs_bucket.blob(gcs_path)

    logging.info(f"Uploading file to GCS: {gcs_path}")
    blob.upload_from_filename(filename)

    # Cleanup
    logging.info(f"Removing local file: {os.getcwd()}/{filename}")
    os.remove(filename)


@task
def create_external_table():
    bq_dataset = "raw"
    bq_table_name = "market_data"
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=bq_dataset,
        table_name=bq_table_name,
        schema_path=None,  # auto-detect schema
        source_uri=f"gs://data-warehouse-harbour/raw/market_data/*",
        partition_uri=f"gs://data-warehouse-harbour/raw/market_data",
        source_format="CSV",
    )


@dag(
    dag_id="market_data",
    start_date=pendulum.datetime(2023, 11, 22, tz="UTC"),
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=True,
    tags=["daily", "external", "pricing"],
)
def market_data():
    """
    The market data pipeline pulls data from an external S3 bucket daily.

    We expect the data to be lagging by one day.
    If the run date is today, then the file we load is uploaded by the external source yesterday.
    """
    load_csv_from_s3_to_gcs() >> create_external_table()


market_data()
