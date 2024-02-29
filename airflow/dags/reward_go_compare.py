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


def _get_gocompare_policies() -> pd.DataFrame:
    gocompare_policies_df = pd.read_gbq(
        """
        select customer_uuid, quote_id, quote_source, policy_start_date, name
        from
            dbt.dim_policy_detail
        where
            quote_source = "gocompare" and policy_start_date >= date("2024-01-01") and date_diff(current_date(), policy_start_date, DAY) >= 55 and (is_subscription_active = True or annual_payment_id is not null)
        """
    )
    return gocompare_policies_df


def _get_gocompare_quotes_between() -> pd.DataFrame:
    return pd.read_gbq(
        f"""
        select quote_uuid, quote_source, quote_at, common_quote
        from
            dbt.dim_quote
        where
            quote_source = "gocompare" and date(quote_at) >= date("2024-01-01") and date(quote_at) <= date("2024-01-31")
        """
    )


def _get_customers_by_id(ids: List[str]) -> pd.DataFrame:
    ids = ["'" + id + "'" for id in ids]
    return pd.read_gbq(
        f"""
        with customer as (
            select * except (effective_from)
            from (
                select customer_uuid, email, first_name, effective_from
                , max(effective_from) over(partition by customer_uuid order by effective_from desc) as _latest
                from dbt.dim_customer
            )
            where effective_from = _latest)

        select customer_uuid, email, first_name,
        from
            customer
        where
            customer_uuid in ({','.join(ids)})
        """
    )


def _get_eligible_customers():
    # Fetch policies created from GoCompare quotes between 1 Jan 2024 and 31 Jan 2024
    # Fetch gocompare policies started after 1 Jan 2024, with an active subscription or an annual payment id

    gocompare_policies_df = _get_gocompare_policies()

    # if this is empty, we don't have any eligible customers
    if gocompare_policies_df.empty:
        logger.info("No eligible policies")
        return None
    gocompare_policies_df = gocompare_policies_df.rename(
        columns={"quote_id": "quote_uuid", "name": "pet_name"}
    )

    # Fetch gocompare quotes created between 1 Jan 2024 and 31 Jan 2024
    gocompare_quotes_df = _get_gocompare_quotes_between()
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

    df.to_csv(
        f"gs://{GCS_BUCKET}/go-compare-rewards/reporting/run_date={date.today()}/eligible_customers.csv"
    )


def _create_redemptions_for_customers():
    customers_df = pd.read_csv(f"gs://{GCS_BUCKET}/go-compare-rewards/reporting/run_date={date.today()}/eligible_customers.csv")
    id_token = _get_promotion_service_id_token()
    for customer in customers_df.to_dict("records"):
        promotion_code = GO_COMPARE_PROMOTION_CODE

        create_redemption = {
            "customer_uuid": customer["customer_uuid"],
            "customer_email": customer["email"],
            "quote_uuid": customer["quote_uuid"],
            "quote_start_date": datetime.strptime(
                (customer["common_quote"]["start_date"]), "%Y-%m-%d"
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
    redemptions = [redemption for redemption in redemptions if redemption["promotion"]["code"] == GO_COMPARE_PROMOTION_CODE]
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
        f"gs://{GCS_BUCKET}/go-compare-rewards/reporting/run_date={date.today()}/report.csv"
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
    schedule_interval="0 10 * * *", # run daily at 10am
    start_date=datetime(2024, 3, 1, 10, 0, 0),
    default_args={"retries": 0},
    tags=["promotion"],
)

def reward_gocompare_customers():
    placeholder = EmptyOperator(task_id="noop")
    (
        placeholder
        >> get_eligible_customers()
        >> create_redemptions_for_customers()
        >> create_report.override(task_id="create_before_report")()
        >> create_rewards()
        >> create_report.override(task_id="create_after_report")()
    )


reward_gocompare_customers()