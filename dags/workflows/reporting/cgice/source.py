from datetime import datetime, timedelta
from typing import Tuple

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from .transform import (
    format_date,
    format_gender,
    format_breed,
    is_multipet,
    format_multipet,
    is_sold,
    is_cancelled,
    is_renewal,
    format_name,
)

JINJA_ENV = Environment(loader=FileSystemLoader("dags/sql/"))
GCP_PROJECT_ID = "ae32-vpcservice-datawarehouse"


def enrich_policy_records(
    policies: pd.DataFrame,
    renewals: pd.DataFrame,
    report_window: Tuple[datetime, datetime],
) -> pd.DataFrame:
    policies["is_sold"] = is_sold(policies.annual_payment_id, policies.subscription_id)
    policies["is_renewal"] = is_renewal(policies.policy_id, renewals.new_policy_id)
    policies["is_cancelled"] = is_cancelled(
        policies.cancel_date, policies.is_sold, report_window
    )
    return policies


def enrich_customer_table(
    customers: pd.DataFrame,
    users: pd.DataFrame,
) -> pd.DataFrame:
    customers = customers.merge(users, how="left", on="user_id").drop_duplicates()
    customers["insured_name"] = format_name(customers.first_name, customers.last_name)
    customers["insured_dob"] = format_date(customers.date_of_birth)
    return customers


def enrich_pet_table(
    pets: pd.DataFrame,
    breeds: pd.DataFrame,
) -> pd.DataFrame:
    pets = pets.merge(breeds, how="left", on="breed_id").drop_duplicates()
    pets["pet_cost"] = pets.cost.map("{:.2f}".format)
    pets["pet_type"] = pets.species
    pets["pet_gender"] = np.vectorize(format_gender)(pets.gender.astype(int))
    pets["pet_breed"] = np.vectorize(format_breed)(pets.breed_name, pets.breed_category)
    pets["is_multipet"] = is_multipet(pets.multipet_number)
    pets["multipet"] = np.vectorize(format_multipet)(pets.is_multipet)
    return pets


def get_policies(
    renewals: pd.DataFrame,
    date_column: str,
    data_window: Tuple[datetime, datetime],
    report_window: Tuple[datetime, datetime],
) -> pd.DataFrame:
    policies = pd.read_gbq(
        JINJA_ENV.get_template("query_columns_by_epoch.sql").render(
            dict(
                table_name="{}.raw.policy".format(GCP_PROJECT_ID),
                columns=[
                    "version_id",
                    "reference_number",
                    "quote_id",
                    "quote_source",
                    "annual_payment_id",
                    "annual_price",
                    "payment_plan_type",
                    "subscription_id",
                    "policy_id",
                    "product_id",
                    "customer_id",
                    "pet_id",
                    "cancel_date",
                    "cancel_reason",
                    "created_date",
                    "start_date",
                    "end_date",
                ],
                date_column=date_column,
                start_date=data_window[0].strftime("%Y-%m-%d"),
                end_date=data_window[1].strftime("%Y-%m-%d"),
            )
        )
    )
    return enrich_policy_records(policies, renewals, report_window)


def get_renewals_quote_source() -> pd.DataFrame:
    return pd.read_gbq(
        JINJA_ENV.get_template("query_renewal_quote_source.sql").render()
    )


def get_dimension_tables(
    start_date: datetime,
    end_date: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    products, renewals, pets, breeds, customers, users = [
        pd.read_gbq(
            JINJA_ENV.get_template("query_columns_by_date.sql").render(
                dict(
                    columns=config["columns"],
                    table_name=config["table_name"],
                    date_column=config.get("date_column"),
                    start_date=config.get("start_date"),
                    end_date=config.get("end_date"),
                )
            )
        )
        for config in [
            dict(
                table_name="{}.raw.product".format(GCP_PROJECT_ID),
                columns=[
                    "id as product_id",
                    "reference as product_reference",
                ],
            ),
            dict(
                table_name="{}.raw.renewal".format(GCP_PROJECT_ID),
                columns=[
                    "new_policy_id",
                    "old_policy_id",
                ],
            ),
            dict(
                table_name="{}.raw.pet".format(GCP_PROJECT_ID),
                columns=[
                    "version_id",
                    "pet_id",
                    "species",
                    "name as pet_name",
                    "breed_id",
                    "breed_category",
                    "age_months",
                    "gender",
                    "cost",
                    "is_microchipped",
                    "is_neutered",
                    "multipet_number",
                ],
            ),
            dict(
                table_name="{}.raw.breed".format(GCP_PROJECT_ID),
                columns=[
                    "id as breed_id",
                    "name as breed_name",
                ],
                date_column="run_date",
                start_date=end_date.strftime("%Y-%m-%d"),
                end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            ),
            dict(
                table_name="{}.raw.customer".format(GCP_PROJECT_ID),
                columns=[
                    "version_id",
                    "customer_id",
                    "user_id",
                    "date_of_birth",
                    "postal_code",
                ],
            ),
            dict(
                table_name="{}.raw.user".format(GCP_PROJECT_ID),
                columns=[
                    "id as user_id",
                    "first_name",
                    "last_name",
                ],
            ),
        ]
    ]
    customers = enrich_customer_table(customers, users)
    pets = enrich_pet_table(pets, breeds)
    return products, renewals, pets, customers
