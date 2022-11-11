from datetime import datetime

from google.cloud import bigquery


def create_napo_benefit_weekly_view(
    project_name: str,
    dataset_name: str,
    src_table: str,
    view_name: str,
    run_date: datetime,
) -> None:
    try:
        bq_client = bigquery.Client(project=project_name)
        dataset_id = "{}.{}".format(project_name, dataset_name)
        dataset_ref = bq_client.get_dataset(dataset_id)
        table = bigquery.Table(dataset_ref.table(view_name))
        table.view_query = """
            select *
            from `{project_name}.{dataset_name}.{table_name}`
            where extract(year from run_date) = {run_year}
            and extract(week from run_date) = {run_week}
        """.format(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=src_table,
            run_year=run_date.strftime("%Y"),
            run_week=run_date.strftime("%V"),
        )
        # exists_ok=True allow updating the view
        bq_client.create_table(table, exists_ok=True)
    except Exception:
        raise


def create_bq_view(
    project_name: str,
    dataset_name: str,
    table_name: str,
    view_name: str,
    run_date: datetime,
) -> None:
    try:
        bq_client = bigquery.Client(project=project_name)
        dataset_id = "{}.{}".format(project_name, dataset_name)
        dataset_ref = bq_client.get_dataset(dataset_id)
        table = bigquery.Table(
            dataset_ref.table(view_name + "_" + run_date.strftime("%Y%m%d"))
        )
        table.view_query = """
            SELECT *
            FROM `{project_name}.{dataset_name}.{table_name}`
            where run_date = '{run_date}'
        """.format(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name,
            run_date=run_date.strftime("%Y-%m-%d"),
        )
        bq_client.create_table(table, exists_ok=True)
    except Exception:
        raise
