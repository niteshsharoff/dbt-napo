import logging
import uuid
from datetime import datetime
from typing import List

import pandas as pd
import pydantic
import requests
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token

from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import task

logger = logging.getLogger(__name__)


def _get_promotion_service_id_token():
    PROMOTION_SERVICE_BASE_URL = Variable.get("PROMOTION_SERVICE_BASE_URL")
    return fetch_id_token(Request(), PROMOTION_SERVICE_BASE_URL)


class CreateCustomer(pydantic.BaseModel):
    uuid: uuid.UUID
    email: str
    is_in_arrears: bool
    first_name: str
    pet_names: list[str]


def _get_gocompare_policies() -> pd.DataFrame:
    gocompare_policies_df = pd.read_gbq(
        """
        select (customer_uuid, quote_id, quote_source, policy_start_date, name)
        from
            dbt.dim_policy_detail
        where
            quote_source = "gocompare" and policy_start_date >= date("2024-01-01") and (is_subscription_active = True or annual_payment_id != null)
        """
    )
    return gocompare_policies_df


def _get_gocompare_quotes_between(start: datetime, end: datetime) -> pd.DataFrame:
    return pd.read_gbq(
        f"""
        select (quote_uuid, quote_source, quote_at, common_quote)
        from
            dbt.dim_quote
        where
            quote_source = "gocompare" and quote_at >= date({start.strftime("%Y-%m-%d")}) and quote_at <= date({end.strftime("%Y-%m-%d")})
        """
    )


def _get_customers_by_id(ids: List[str]) -> pd.DataFrame:
    return pd.read_gbq(
        f"""
        select (customer_uuid, email, first_name)
        from
            dbt.dim_customer
        where
            customer_uuid in {ids}
        """
    )


def _get_eligible_customers() -> pd.DataFrame:
    # Fetch policies created from GoCompare quotes between 1 Jan 2024 and 31 Jan 2024
    # Fetch gocompare policies started after 1 Jan 2024, with an active subscription or an annual payment id

    gocompare_policies_df = _get_gocompare_policies()
    gocompare_policies_df = gocompare_policies_df.rename(
        columns={"quote_id": "quote_uuid", "name": "pet_name"}
    )
    # Fetch gocompare quotes created between 1 Jan 2024 and 31 Jan 2024
    gocompare_quotes_df = _get_gocompare_quotes_between(
        datetime(2024, 1, 1), datetime(2024, 1, 31)
    )
    # inner join on quote uuid to only get active policies that were purchased from quotes given between 1 Jan and 31 Jan
    eligible_policies_df = gocompare_policies_df.merge(
        gocompare_quotes_df, left_on="quote_uuid", right_on="quote_uuid", how="inner"
    )
    eligible_customers_df = _get_customers_by_id(
        list(eligible_policies_df["customer_uuid"])
    )

    pets_by_customer = (
        eligible_policies_df.groupby(["customer_uuid", "quote_uuid"])["pet_name"]
        .apply(list)
        .reset_index()
    )

    # merge pets with customer df
    customers_df = eligible_customers_df.merge(
        pets_by_customer, how="inner", left_on="customer_uuid", right_on="customer_uuid"
    )
    df = customers_df.merge(
        gocompare_quotes_df, how="inner", left_on="quote_uuid", right_on="quote_uuid"
    )

    return df


def _create_redemptions_for_customers(customers_df):
    PROMOTION_SERVICE_BASE_URL = Variable.get("PROMOTION_SERVICE_BASE_URL")
    id_token = _get_promotion_service_id_token()
    for customer in customers_df.to_dict("records"):
        promotion_code = "GOCOMPAREJAN2024"

        create_redemption = {
            "customer_uuid": customer["customer_uuid"],
            "customer_email": customer["email"],
            "quote_uuid": customer["quote_uuid"],
            "quote_start_date": datetime.strptime(
                (customer["common_quote"]["start_date"]), "%Y-%m-%d %H:%M:%S"
            ).strftime("%Y-%m-%d"),
            "has_active_policies": True,
            "customer_is_in_arrears": False,
            "customer_first_name": customer["first_name"],
            "customer_pet_names": customer["pet_name"],
        }

        response = requests.post(
            f"{PROMOTION_SERVICE_BASE_URL}/promotions/{promotion_code}/redemption",
            json=create_redemption,
            headers={"Authorization": f"Bearer {id_token}"},
        )
        if response.status_code != 200:
            if response.status_code == 400:
                logger.warning(
                    "Received client error from promotion service for "
                    f"quote {create_redemption['quote_uuid']}: {response.content}"
                )
            elif response.status_code == 500:
                logger.warning(
                    "Received server error from promotion service for "
                    f"quote {create_redemption['quote_uuid']}: {response.content}"
                )
            else:
                logger.error(
                    "Received unexpected error from promotion service "
                    f"for quote {create_redemption['quote_uuid']}: {response.content}"
                )
        else:
            logger.info(f"Received success for quote {create_redemption['quote_uuid']}")


@task(task_id="get_eligible_customers")
def get_eligible_customers():
    return _get_eligible_customers()


@task(task_id="create_redemptions")
def create_redemptions_for_customers(customers_df):
    return _create_redemptions_for_customers(customers_df)


@dag(
    dag_id="reward_gocompare_customers",
    schedule_interval=None,
    start_date=datetime(2023, 7, 19, 0, 0, 0),
    default_args={"retries": 0},
    tags=["promotion"],
)
def reward_gift_card_promotions():
    placeholder = EmptyOperator(task_id="noop")
    (placeholder >> get_eligible_customers())
