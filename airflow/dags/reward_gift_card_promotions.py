from datetime import datetime, date
from typing import Any
import urllib.parse as urlparse

import pandas as pd
import requests
import logging

from google.oauth2.id_token import fetch_id_token
from google.auth.transport.requests import Request

from airflow import AirflowException
from airflow.models.dag import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import task

PROMOTION_SERVICE_BASE_URL = "https://promotion-api-hcheiihzga-nw.a.run.app"
GCS_BUCKET = Variable.get("GCS_BUCKET")

logger = logging.getLogger(__name__)


def _get_promotion_service_id_token():
    return fetch_id_token(Request(), PROMOTION_SERVICE_BASE_URL)


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


@task(task_id="create_redemptions")
def create_redemptions():
    # Exclude MSENAPO22 from redemptions as it was run incorrectly and had
    # to be redeemed manually
    create_redemptions_df = pd.read_gbq("""
        select
            * except(quote_start_date),
            format_date("%Y-%m-%d", quote_start_date) as quote_start_date
        from
            dbt_marts.promotion__gift_card_promotion_redemptions
        where
            promotion_code != 'MSENAPO22'
        order by
            quote_start_date
    """)
    id_token = _get_promotion_service_id_token()
    for create_redemption in create_redemptions_df.to_dict("records"):
        promotion_code = create_redemption.pop("promotion_code")
        create_redemption["customer_pet_names"] = create_redemption["customer_pet_names"].tolist()
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
                "amount_due_pence": redemption["promotion"]["value_pence"]
                if redemption["is_eligible_for_reward"] and not reward
                else 0,
                "amount_rewarded_gbp": redemption["promotion"]["value_pence"]
                if reward
                else 0,
            }
        )
    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(
        f"gs://{GCS_BUCKET}/reporting/gift_card_promotions/run_date={date.today()}/report.csv"
    )


@task(task_id="check_runa_balance")
def check_runa_balance():
    report_df = pd.read_csv(
        f"gs://{GCS_BUCKET}/reporting/gift_card_promotions/run_date={date.today()}/report.csv"
    )
    amount_due_gbp = round(report_df["amount_due_pence"].sum() / 100, 2)
    raise AirflowException(
        f"Check that the £{amount_due_gbp} due is available in Runa and then"
        " mark as success to continue"
    )


@task(task_id="create_rewards")
def create_rewards():
    # Assume that any redemptions for quotes prior to 2023-05-01 that are unrewarded in
    # our system were manually rewarded by COps. If this is not the case then COps can
    # reward through the ReTool promotions app
    create_rewards_df = pd.read_gbq("""
        select
            quote_uuid,
            has_active_policies as redemption_has_active_policies,
            customer_is_in_arrears
        from
            dbt_marts.promotion__gift_card_promotion_redemptions
        where
            quote_start_date >= DATE(2023, 5, 1)
        order by
            quote_start_date
    """)
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
        create_reward["customer_is_in_arrears"] = bool(create_reward["customer_is_in_arrears"])
        create_reward["redemption_has_active_policies"] = bool(create_reward["redemption_has_active_policies"])
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
    dag_id="reward_gift_card_promotions",
    schedule_interval=None,
    start_date=datetime(2023, 7, 19, 0, 0, 0),
    default_args={"retries": 0},
    tags=["promotion"],
)
def reward_gift_card_promotions():
    placeholder = EmptyOperator(task_id="noop")
    (
        placeholder
        >> create_redemptions()
        >> create_report.override(task_id="create_before_report")()
        >> check_runa_balance()
        >> create_rewards()
        >> create_report.override(task_id="create_after_report")()
    )


reward_gift_card_promotions()
