import ast
import logging
from urllib.parse import urlparse
import uuid
from datetime import date, datetime
from typing import Any, List

import pandas as pd
import pydantic
import requests
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token

from airflow.models import Variable
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import task
from airflow import AirflowException

logger = logging.getLogger(__name__)

PROMOTION_SERVICE_BASE_URL = "https://promotion-api-hcheiihzga-nw.a.run.app"
GCS_BUCKET = Variable.get("GCS_BUCKET")
GO_COMPARE_PROMOTION_CODE = "GOCOMPAREJAN2024"


def _paginate_url(url, headers={}):
    items = []
    page_number = 1
    url_parts = urlparse.urlparse(url)
    while True:
        page_url_parts = urlparse.ParseResult(
            scheme=url_parts.scheme,
            netloc=url_parts.netloc,
            path=url_parts.path,
            params=url_parts.params,
            query=urlparse.urlencode(
                {
                    **dict(urlparse.parse_qsl(url_parts.query)),
                    "page": page_number,
                }
            ),
            fragment=url_parts.fragment,
        )
        response = requests.get(urlparse.urlunparse(page_url_parts), headers=headers)
        response.raise_for_status()
        response_body = response.json()
        if len(response_body["items"]) == 0:
            break
        page_number += 1
        items += response_body["items"]
    return items


def _get_promotion_service_id_token():
    return fetch_id_token(Request(), PROMOTION_SERVICE_BASE_URL)


def _get_eligible_customers():
    # Fetch policies created from GoCompare quotes between 1 Jan 2024 and 31 Jan 2024
    # Fetch gocompare policies started after 1 Jan 2024, with an active subscription or an annual payment id

    gocompare_eligible_policies_df = pd.read_gbq(
        """
        select customer.uuid, customer.email, quote.quote_id, policy.start_date, customer.first_name, pet.name, quote.created_at from (
            select * except (row_effective_from)
            from (
                select *
                , max(row_effective_from) over(partition by policy.policy_id) as _latest
                from `ae32-vpcservice-datawarehouse.dbt.int_policy_history_v2`
            )
            where row_effective_from = _latest)
        where
            quote.created_at >= 1704067200000
            and quote.created_at <= 1706659200000
            and quote.source = "tungsten-vale"
            and policy.start_date >= date("2024-01-01")
            and date_diff(current_date(), policy.start_date, DAY) = 60
            and policy.sold_at is not null and policy.cancel_date is null
        """
    )

    # if this is empty, we don't have any eligible customers
    if gocompare_eligible_policies_df.empty:
        logger.info("No eligible policies")
        return None

    # renaming columns
    gocompare_eligible_policies_df = gocompare_eligible_policies_df.rename(
        columns={
            "quote_id": "quote_uuid",
            "name": "pet_name",
            "uuid": "customer_uuid",
            "created_at": "quote_created_at",
        }
    )

    # group by customer and quote
    gocompare_eligible_policies_df = (
        gocompare_eligible_policies_df.groupby(["customer_uuid", "quote_uuid"])
        .agg(list)
        .reset_index()
    )

    # explode lists of first name, email, start_date, and quote_created_at, to remove list structure
    gocompare_eligible_policies_df = gocompare_eligible_policies_df.explode(
        "first_name"
    )
    gocompare_eligible_policies_df = gocompare_eligible_policies_df.explode("email")
    gocompare_eligible_policies_df = gocompare_eligible_policies_df.explode(
        "start_date"
    )
    gocompare_eligible_policies_df = gocompare_eligible_policies_df.explode(
        "quote_created_at"
    )
    # drop any duplicates that exploding may have made
    gocompare_eligible_policies_df = gocompare_eligible_policies_df.drop_duplicates(
        subset=[
            "customer_uuid",
            "quote_uuid",
        ]
    )
    gocompare_eligible_policies_df.to_csv(
        f"gs://{GCS_BUCKET}/go-compare-rewards/reporting/run_date={date.today()}/eligible_customers.csv",
        index=False,
    )


def _create_redemptions_for_customers():
    eligible_policies_df = pd.read_csv(
        f"gs://{GCS_BUCKET}/go-compare-rewards/reporting/run_date={date.today()}/eligible_customers.csv"
    )
    id_token = _get_promotion_service_id_token()
    for policy in eligible_policies_df.to_dict("records"):
        promotion_code = GO_COMPARE_PROMOTION_CODE

        create_redemption = {
            "customer_uuid": policy["customer_uuid"],
            "customer_email": policy["email"],
            "quote_uuid": policy["quote_uuid"],
            "quote_start_date": policy["start_date"],
            "has_active_policies": True,
            "customer_is_in_arrears": False,
            "customer_first_name": policy["first_name"],
            "customer_pet_names": policy["pet_name"],
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
                message = f"Received unexpected error from promotion service for quote {create_redemption['quote_uuid']}: {response.content}"
                logger.error(message)
        else:
            logger.info(f"Received success for quote {create_redemption['quote_uuid']}")


@task(task_id="create_report")
def create_report():
    id_token = _get_promotion_service_id_token()
    redemptions = _paginate_url(
        f"{PROMOTION_SERVICE_BASE_URL}/redemptions/",
        headers={"Authorization": f"Bearer {id_token}"},
    )
    rewards = _paginate_url(
        f"{PROMOTION_SERVICE_BASE_URL}/rewards/",
        headers={"Authorization": f"Bearer {id_token}"},
    )
    reward_lookup = {reward["redemption"]["uuid"]: reward for reward in rewards}
    report_rows: list[dict[str, Any]] = []
    redemptions = [
        redemption
        for redemption in redemptions
        if redemption["promotion"]["code"] == GO_COMPARE_PROMOTION_CODE
    ]
    for redemption in redemptions:
        reward = reward_lookup.get(redemption["uuid"])
        report_rows.append(
            {
                "customer_email": redemption["customer"]["email"],
                "promotion_code": redemption["promotion"]["code"],
                "policy_started_on": redemption["quote_start_date"],
                "rewardable_on": redemption["rewardable_on"],
                "overdue_on": redemption["overdue_on"],
                "has_active_policies": redemption["has_active_policies"],
                "customer_is_in_arrears": redemption["customer"]["is_in_arrears"],
                "is_eligible_for_reward": redemption["is_eligible_for_reward"],
                "amount_due_pence": (
                    redemption["promotion"]["value_pence"]
                    if redemption["is_eligible_for_reward"] and not reward
                    else 0
                ),
                "amount_rewarded_gbp": (
                    redemption["promotion"]["value_pence"] if reward else 0
                ),
            }
        )
    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(
        f"gs://{GCS_BUCKET}/go-compare-rewards/reporting/run_date={date.today()}/report.csv",
        index=False,
    )


@task(task_id="get_eligible_customers")
def get_eligible_customers():
    return _get_eligible_customers()


@task(task_id="create_redemptions")
def create_redemptions_for_customers():
    return _create_redemptions_for_customers()


@task(task_id="create_rewards")
def create_rewards():
    create_rewards_df = pd.read_gbq(
        f"""
        select
            quote_uuid,
            has_active_policies as redemption_has_active_policies,
            customer_is_in_arrears
        from
            dbt_marts.promotion__gift_card_promotion_redemptions
        where
            promotion_code = '{GO_COMPARE_PROMOTION_CODE}'
        order by
            quote_start_date
    """
    )
    create_rewards_lookup = create_rewards_df.set_index("quote_uuid").to_dict("index")

    id_token = _get_promotion_service_id_token()
    redemptions = _paginate_url(
        f"{PROMOTION_SERVICE_BASE_URL}/redemptions/",
        headers={"Authorization": f"Bearer {id_token}"},
    )
    for redemption in redemptions:
        if redemption["quote_uuid"] not in create_rewards_lookup:
            continue
        create_reward = create_rewards_lookup[redemption["quote_uuid"]]
        create_reward["customer_is_in_arrears"] = bool(
            create_reward["customer_is_in_arrears"]
        )
        create_reward["redemption_has_active_policies"] = bool(
            create_reward["redemption_has_active_policies"]
        )
        response = requests.post(
            f"{PROMOTION_SERVICE_BASE_URL}/redemptions/{redemption['uuid']}/reward",
            json=create_reward,
            headers={"Authorization": f"Bearer {id_token}"},
        )
        if response.status_code != 200:
            if response.status_code == 400:
                logger.warning(
                    "Received client error from promotion service whilst "
                    f"rewarding redemption {redemption['uuid']}: {response.content}"
                )
            elif response.status_code == 500:
                logger.warning(
                    "Received server error from promotion service whilst "
                    f"rewarding redemption {redemption['uuid']}: {response.content}"
                )
            else:
                logger.error(
                    "Received unexpected error from promotion service whilst "
                    f"rewarding redemption {redemption['uuid']}: {response.content}"
                )
        else:
            logger.info(f"Received success whilst rewarding {redemption['uuid']}")


@dag(
    dag_id="reward_gocompare_customers",
    schedule_interval="0 10 * * *",  # run daily at 10am
    start_date=datetime(2024, 3, 1, 10, 0, 0),
    default_args={"retries": 0},
    tags=["promotion"],
)
def reward_gocompare_customers():
    (
        get_eligible_customers()
        >> create_redemptions_for_customers()
        >> create_report.override(task_id="create_before_report")()
        >> create_rewards()
        >> create_report.override(task_id="create_after_report")()
    )


reward_gocompare_customers()
