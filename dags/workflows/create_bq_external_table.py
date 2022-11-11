import logging
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import HivePartitioningOptions, CSVOptions
from google.cloud.exceptions import NotFound, Conflict

DATASET_ID = "{project}.{dataset}"


def create_bq_dataset(
    bq_client: bigquery.Client,
    dataset_id: str,
    region: str,
) -> bigquery.Dataset:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = region
    dataset = bq_client.create_dataset(dataset, timeout=30)
    return dataset


def create_external_bq_table(
    project_name: str,
    region: str,
    dataset_name: str,
    table_name: str,
    schema_path: Optional[str],
    source_uri: str,
    partition_uri: str,
    source_format: str,
    skip_leading_rows: Optional[int] = 1,
) -> None:
    log = logging.getLogger(__name__)
    bq_client = bigquery.Client(project=project_name)
    dataset_id = DATASET_ID.format(project=project_name, dataset=dataset_name)
    try:
        dataset_ref = bq_client.get_dataset(dataset_id)
        log.info("Dataset '{}' already exists".format(dataset_id))
    except NotFound:
        log.info("Creating dataset '{}'".format(dataset_id))
        dataset_ref = create_bq_dataset(bq_client, dataset_id, region)

    # Default to JSON
    table_config = bigquery.ExternalConfig(
        bigquery.ExternalSourceFormat.NEWLINE_DELIMITED_JSON
    )

    if source_format == "CSV":
        table_config = bigquery.ExternalConfig(bigquery.ExternalSourceFormat.CSV)
        csv_options = CSVOptions()
        csv_options.skip_leading_rows = skip_leading_rows
        csv_options.allow_quoted_newlines = True

    hive_partitioning_options = HivePartitioningOptions()
    hive_partitioning_options.mode = "AUTO"
    hive_partitioning_options.source_uri_prefix = partition_uri

    table_config.source_uris = [source_uri]
    table_config.hive_partitioning = hive_partitioning_options
    table_config.max_bad_records = 0
    table_config.autodetect = True

    try:
        if schema_path:
            table_config.autodetect = False
            table_schema = bq_client.schema_from_json(schema_path)
            table = bigquery.Table(dataset_ref.table(table_name), schema=table_schema)
        else:
            table = bigquery.Table(dataset_ref.table(table_name))

        log.info("Creating table '{}.{}'".format(dataset_id, table_name))
        table.external_data_configuration = table_config
        bq_client.create_table(table)

    except Conflict:
        if schema_path:
            table_config.autodetect = False
            table_schema = bq_client.schema_from_json(schema_path)
            # Append run_date partition key to schema if table exists in BQ
            table_schema.append(bigquery.SchemaField("run_date", "DATE"))
            table = bigquery.Table(dataset_ref.table(table_name), schema=table_schema)
        else:
            table = bigquery.Table(dataset_ref.table(table_name))

        log.info("Updating table '{}.{}'".format(dataset_id, table_name))
        table.external_data_configuration = table_config
        bq_client.update_table(table, fields=["schema", "external_data_configuration"])
