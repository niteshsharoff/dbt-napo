import pendulum
from jinja2 import Environment, FileSystemLoader

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import dag
from dags.workflows.create_bq_view import create_bq_view
from dags.workflows.export_bq_result_to_gcs import export_query_to_gcs

JINJA_ENV = Environment(loader=FileSystemLoader("dags/"))
OFFER_RENEWAL_DISCOUNTS_QUERY = JINJA_ENV.get_template(
    "sql/renewal_service/offer_discount.sql"
)

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCS_BUCKET = "data-warehouse-harbour"
BQ_DATASET = "renewal_service"


@task
def create_eligible_discounts_view(data_interval_end: pendulum.datetime = None):
    run_date = data_interval_end
    target_renewal_date = run_date.add(months=1)
    create_bq_view(
        project_name=GCP_PROJECT_ID,
        dataset_name=BQ_DATASET,
        view_name=f"offered_renewal_discounts_{run_date.format('YYYYMMDD')}",
        view_query=OFFER_RENEWAL_DISCOUNTS_QUERY.render(
            dict(
                renewal_date=target_renewal_date.format("YYYY-MM-DD"),
            )
        ),
    )


@task
def export_eligible_discounts_to_gcs(data_interval_end: pendulum.datetime = None):
    run_date = data_interval_end
    table_name = f"offered_renewal_discounts_{run_date.format('YYYYMMDD')}"
    export_query_to_gcs(
        project_name=GCP_PROJECT_ID,
        query=f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`",
        gcs_bucket=GCS_BUCKET,
        gcs_uri="{}/offered_renewal_discounts/run_date={}/{}".format(
            BQ_DATASET,
            run_date.date(),
            table_name,
        ),
        replace_column_underscores=False,
    )


@dag(
    dag_id="renewal_service",
    start_date=pendulum.datetime(2023, 9, 6, tz="UTC"),
    schedule_interval="0 8 * * *",
    catchup=True,
    default_args={"retries": 0},
    max_active_runs=1,
    max_active_tasks=7,
    tags=["renewal"],
)
def renewal_service_pipeline():
    create_eligible_discounts_view() >> export_eligible_discounts_to_gcs()


renewal_service_pipeline()
