from datetime import datetime

import pytz

from airflow import DAG, macros
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.decorators import task, task_group
from airflow.models import Variable
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

from workflows.create_bq_view import create_bq_view
from workflows.create_bq_external_table import create_external_bq_table
from workflows.reporting.cgice_premium_monthly import generate_monthly_premium_bdx


GCP_CREDENTIALS = Variable.get("GCP_CREDENTIALS")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")


@task(task_id="generate_monthly_report")
def generate_report(ds=None):
    start_date = macros.datetime.strptime(ds, "%Y-%m-%d")
    dt = macros.dateutil.relativedelta.relativedelta(months=1)
    generate_monthly_premium_bdx(
        datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=pytz.UTC),
        (start_date + dt).replace(tzinfo=pytz.UTC),
    )


@task(task_id="create_monthly_table")
def create_table(dataset_name: str, table_name: str, ds=None):
    create_external_bq_table(
        project_name=GCP_PROJECT_ID,
        region=GCP_REGION,
        dataset_name=dataset_name,
        table_name=table_name,
        source_uri=f"gs://{GCS_BUCKET}/reporting/{table_name}/*",
        partition_uri=f"gs://{GCS_BUCKET}/reporting/{table_name}",
        source_format="CSV",
        schema_path=None,
    )


@task(task_id="create_monthly_view")
def create_view(dataset_name: str, table_name: str, view_name: str, ds=None):
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=dataset_name,
        table_name=table_name,
        view_name=view_name,
        run_date=datetime.strptime(ds, "%Y-%m-%d"),
    )


with DAG(
    dag_id="cgice_premium_monthly",
    start_date=pendulum.datetime(2021, 9, 1, tz="UTC"),
    schedule_interval="0 0 1 * *",
    catchup=True,
    default_args={"retries": 0},
) as dag:
    start_date = "{{ ds }}"
    end_date = """{{ (macros.datetime.strptime(ds, "%Y-%m-%d") 
        + macros.dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d") 
    }}"""

    @task_group(group_id="data_sources")
    def setup_sensors():
        for table, version, gcs_prefix in [
            ("policy", "1.0.0", "policy_policy"),
            ("pet", "1.0.0", "policy_pet"),
            ("breed", "1.0.0", "policy_breed"),
            ("customer", "1.0.0", "policy_customer"),
            ("user", "1.0.0", "auth_user"),
        ]:
            GoogleCloudStorageObjectSensor(
                task_id=f"wait_for_{table}_table_partition",
                bucket=GCS_BUCKET,
                object=f"raw/{gcs_prefix}/{version}/run_date={end_date}/data.json",
                poke_interval=15 * 60,  # Check gcs every 15 minutes
                timeout=60
                * 60,  # Fail the pipeline after if latest partition not found
            )

    @task_group(group_id="big_query")
    def create_table_and_view():
        t1 = create_table("reporting", "cgice_premium_monthly")
        t2 = create_view(
            "reporting",
            "cgice_premium_monthly",
            "napo_pet_premium_bdx",
        )
        t1 >> t2

    setup_sensors() >> generate_report() >> create_table_and_view()
