import logging
from datetime import datetime

from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader


JINJA_ENV = Environment(loader=FileSystemLoader("dags/sql/"))


def create_pcw_sales_view(
    project_name: str,
    dataset_name: str,
    src_table: str,
    view_name: str,
    start_date: datetime,
    end_date: datetime,
) -> None:
    bq_client = bigquery.Client(project=project_name)
    dataset_id = "{}.{}".format(project_name, dataset_name)
    dataset_ref = bq_client.get_dataset(dataset_id)
    table = bigquery.Table(dataset_ref.table(view_name))
    table.view_query = """
        select * except(run_date)
        from `{project_name}.{dataset_name}.{table_name}` 
        where run_date > date('{start_date}', 'UTC')
        and run_date <= date('{end_date}', 'UTC')
    """.format(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=src_table,
        start_date=start_date,
        end_date=end_date,
    )
    bq_client.create_table(table, exists_ok=True)


def create_ctm_sales_monthly_view(
    project_name: str,
    dataset_name: str,
    src_table: str,
    view_name: str,
    start_date: datetime,
    end_date: datetime,
) -> None:
    bq_client = bigquery.Client(project=project_name)
    dataset_id = "{}.{}".format(project_name, dataset_name)
    dataset_ref = bq_client.get_dataset(dataset_id)
    table = bigquery.Table(dataset_ref.table(view_name))
    table.view_query = """
        select * except(run_date)
        from `{project_name}.{dataset_name}.{table_name}` 
        where parse_datetime('{format}', {column}) >= date('{start_date}', 'UTC')
        and parse_datetime('{format}', {column}) < date('{end_date}', 'UTC')
        order by Transaction_Datetime desc
    """.format(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=src_table,
        start_date=start_date,
        end_date=end_date,
        column="Transaction_Datetime",
        format="%d/%m/%Y %H:%M:%S",
    )
    bq_client.create_table(table, exists_ok=True)


def create_bq_view(
    project_name: str,
    dataset_name: str,
    view_name: str,
    view_query: str,
) -> None:
    try:
        logging.info("Creating view with query:\n" + view_query)
        bq_client = bigquery.Client(project=project_name)
        dataset_id = "{}.{}".format(project_name, dataset_name)
        dataset_ref = bq_client.get_dataset(dataset_id)
        table = bigquery.Table(dataset_ref.table(view_name))
        table.view_query = view_query
        bq_client.create_table(table, exists_ok=True)
    except Exception:
        raise AirflowFailException


def create_quotezone_sales_view(
    project_name: str,
    dataset_name: str,
    view_name: str,
    start_date: datetime,
    end_date: datetime,
    snapshot_at: datetime,
) -> None:
    bq_client = bigquery.Client(project=project_name)
    dataset_id = "{}.{}".format(project_name, dataset_name)
    dataset_ref = bq_client.get_dataset(dataset_id)
    table = bigquery.Table(dataset_ref.table(view_name))
    table.view_query = JINJA_ENV.get_template("quotezone_sales_report.sql").render(
        dict(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            snapshot_at=snapshot_at.strftime("%Y-%m-%d"),
        )
    )
    bq_client.create_table(table, exists_ok=True)
