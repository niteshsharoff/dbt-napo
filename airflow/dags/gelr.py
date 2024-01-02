import logging
from typing import Optional

import pendulum
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from airflow.models.dag import dag
from airflow.models import Variable
from airflow.operators.python import task
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from dags.workflows.create_bq_external_table import create_bq_table

JINJA_ENV = Environment(loader=FileSystemLoader("dags/sql/gelr"))

EXTERNAL_TABLE_NAME = (
    "ae32-vpcservice-datawarehouse.airflow.policy_claim_snapshot"
)
PARTITION_KEY = "snapshot_date"
PROJECT_NAME = "ae32-vpcservice-datawarehouse"

DBT_CLOUD_JOB_ID = Variable.get("DBT_CLOUD_TEST_JOB_ID")


@task
def create_bq_partitioned_table():
    project_name, dataset_name, table_name = EXTERNAL_TABLE_NAME.split(".")
    create_bq_table(
        project_name=project_name,
        region="EU",
        dataset_name=dataset_name,
        table_name=table_name,
        schema_path="dags/schemas/gelr/policy_claim_snapshot/bq_schema.json",
    )


@task
def delete_existing_data_in_partition(
    data_interval_end: Optional[pendulum.DateTime] = None,
):
    run_date = data_interval_end.date().format("YYYY-MM-DD")
    client = bigquery.Client(project=PROJECT_NAME)
    query = """
        delete from `{table_name}`
        where snapshot_date = '{run_date}'
    """.format(
        table_name=EXTERNAL_TABLE_NAME, run_date=run_date
    )
    query_job = client.query(query)
    query_job.result()

    logging.info(f"query result: {query_job.error_result}")


@task
def insert_data_from_query(data_interval_end: Optional[pendulum.DateTime] = None):
    run_date = data_interval_end.date().format("YYYY-MM-DD")
    client = bigquery.Client(project=PROJECT_NAME)
    query = JINJA_ENV.get_template("load_policy_claim_snapshot.sql").render(
        table_name=EXTERNAL_TABLE_NAME, run_date=run_date
    )
    query_job = client.query(query)
    query_job.result()

    logging.info(f"query result: {query_job.error_result}")


@dag(
    dag_id="gelr",
    start_date=pendulum.datetime(2021, 10, 18, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=7,
    tags=["pricing", "gelr", "daily"],
)
def gelr():
    dbt_checks = DbtCloudRunJobOperator(
        task_id="dbt_checks",
        job_id=DBT_CLOUD_JOB_ID,
        check_interval=10,
        timeout=300,
        trigger_rule="one_success",
    )
    (
        dbt_checks
        >> create_bq_partitioned_table()
        >> delete_existing_data_in_partition()
        >> insert_data_from_query()
    )


gelr()
