from airflow.models.dag import dag
from airflow.models import Variable
from airflow.operators.python import task
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
import pendulum

DBT_CLOUD_JOB_ID = Variable.get("DBT_CLOUD_TEST_JOB_ID")


@dag(
    dag_id="dbt",
    start_date=pendulum.datetime(2024, 1, 31, tz="UTC"),
    schedule_interval="15 1 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["engineering", "dbt", "daily"],
)
def dbt():
    DbtCloudRunJobOperator(
        task_id="dbt_test",
        job_id=DBT_CLOUD_JOB_ID,
        check_interval=10,
        timeout=600,
    )


dbt()