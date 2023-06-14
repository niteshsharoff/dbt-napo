import csv
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.cloud import bigquery, storage


def export_query_to_table(
    bq_client: bigquery.Client,
    project_name: str,
    dataset_name: str,
    table_name: str,
    query: str,
    region: str = "EU",
):
    """
    Write query results to a temp table in Big Query. The temp table is set to expire
    after 1 hour.

    :param bq_client: Big Query client
    :param project_name: GCP project ID
    :param dataset_name: Big Query dataset name
    :param table_name: Source table to execute query against
    :param query: SQL DML statement
    :param region: GCP region for source data and Big Query table
    """
    dataset_ref = bigquery.DatasetReference(project_name, dataset_name)
    table_ref = dataset_ref.table(table_name)

    # Write query results to a temp table
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query_job = bq_client.query(query, job_config=job_config, location=region)
    query_job.result()

    # Set table expiration time
    table = bq_client.get_table(table_ref)
    table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    bq_client.update_table(table, ["expires"])


def export_results_to_gcs(
    bq_client: bigquery.Client,
    project_name: str,
    dataset_name: str,
    table_name: str,
    gcs_uri: str,
    region: str = "EU",
):
    """
    Extract job stores temp table on Google Cloud Storage.

    :param bq_client: Big Query client
    :param project_name: GCP project ID
    :param dataset_name: Big Query dataset name
    :param table_name: Source table to execute query against
    :param gcs_uri: Destination GCS URI
    :param region: GCP region for source data and Big Query table
    """
    dataset_ref = bigquery.DatasetReference(project_name, dataset_name)
    table_ref = dataset_ref.table(table_name)

    # Export table to GCS
    extract_job = bq_client.extract_table(table_ref, gcs_uri, location=region)
    extract_job.result()


def export_table_to_gcs(
    project_name: str,
    dataset_name: str,
    src_table: str,
    tmp_table: str,
    gcs_uri: str,
):
    """
    Downloads a file from Cloud Storage and uploads it to a folder on Google Drive.

    :param project_name: GCP project ID
    :param dataset_name: Big Query Dataset name
    :param src_table: Table to query
    :param tmp_table: Intermediate table for query results
    :param gcs_uri: Destination URI for exported results
    """
    bq_client = bigquery.Client(project=project_name)
    export_query_to_table(
        bq_client,
        project_name,
        dataset_name,
        tmp_table,
        f"SELECT * FROM `{dataset_name}.{src_table}`",
    )
    export_results_to_gcs(bq_client, project_name, dataset_name, tmp_table, gcs_uri)


def export_query_to_gcs(
    project_name: str,
    query: str,
    gcs_bucket: str,
    gcs_uri: str,
    encoding: str = "utf-8",
):
    """
    Write query results to a temp table in Big Query. The temp table is set to expire
    after 1 hour.

    :param project_name: GCP project ID
    :param query: Big Query dataset name
    :param gcs_bucket: Source table to execute query against
    :param gcs_uri: SQL DML statement
    :param encoding: Uploaded csv file encoding
    """
    df = pd.read_gbq(query)
    df = df.rename(columns=lambda x: x.replace("_", " "))  # update column names

    storage_client = storage.Client(project=project_name)
    bucket = storage_client.get_bucket(gcs_bucket)
    content = df.to_csv(index=False, quoting=csv.QUOTE_ALL).encode(encoding)
    blob = bucket.blob(gcs_uri)
    blob.upload_from_string(content, content_type="application/octet-stream")
