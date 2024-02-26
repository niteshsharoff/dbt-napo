import os
from unittest import mock
from unittest.mock import Mock, call

from requests import Response

from airflow.models import Variable
from dags.reward_go_compare import (
    _create_redemptions_for_customers,
    _get_eligible_customers,
)
from tests.make import *


def test_create_redemptions(mocker: MockerFixture):
    with mock.patch.dict(
        "os.environ",
        AIRFLOW_VAR_PROMOTION_SERVICE_BASE_URL="test-promotion-service.com",
    ):
        print(Variable.get("PROMOTION_SERVICE_BASE_URL"))
        eligible_customers = pd.DataFrame(
            [
                [
                    "f3dfd7ac-ef34-434b-82eb-1bae37a15487",
                    "e0ccacea-7695-43a7-8348-fe5a8fa74050",
                    "sam@test.com",
                    "Sam",
                    "gocompare",
                    datetime(2024, 1, 1),
                    {"start_date": "2024-01-01 00:00:00"},
                    list(["Fido"]),
                ],
                [
                    "6af8bb71-eb51-4120-985b-384493e8af62",
                    "b1bb478a-59b3-49a0-aadc-63732f1177f5",
                    "linda@test.com",
                    "Linda",
                    "gocompare",
                    datetime(2024, 1, 1),
                    {"start_date": "2024-01-01 00:00:00"},
                    list(["Rex"]),
                ],
            ],
            columns=[
                "customer_uuid",
                "quote_uuid",
                "email",
                "first_name",
                "quote_source",
                "quote_at",
                "common_quote",
                "pet_name",
            ],
        )

        mocked_promotion_service = mocker.patch("dags.reward_go_compare.requests.post")

        mocked_id_token = mocker.patch(
            "dags.reward_go_compare._get_promotion_service_id_token"
        )

        _create_redemptions_for_customers(eligible_customers)

        calls = [
            call(
                f"test-promotion-service.com/promotions/GOCOMPAREJAN2024/redemption",
                json={
                    "customer_uuid": "f3dfd7ac-ef34-434b-82eb-1bae37a15487",
                    "customer_email": "sam@test.com",
                    "quote_uuid": "e0ccacea-7695-43a7-8348-fe5a8fa74050",
                    "quote_start_date": "2024-01-01",
                    "has_active_policies": True,
                    "customer_is_in_arrears": False,
                    "customer_first_name": "Sam",
                    "customer_pet_names": ["Fido"],
                },
                headers={"Authorization": f"Bearer {mocked_id_token.return_value}"},
            ),
            call(
                f"test-promotion-service.com/promotions/GOCOMPAREJAN2024/redemption",
                json={
                    "customer_uuid": "6af8bb71-eb51-4120-985b-384493e8af62",
                    "customer_email": "linda@test.com",
                    "quote_uuid": "b1bb478a-59b3-49a0-aadc-63732f1177f5",
                    "quote_start_date": "2024-01-01",
                    "has_active_policies": True,
                    "customer_is_in_arrears": False,
                    "customer_first_name": "Linda",
                    "customer_pet_names": ["Rex"],
                },
                headers={"Authorization": f"Bearer {mocked_id_token.return_value}"},
            ),
        ]

        mocked_promotion_service.assert_has_calls(calls, any_order=True)


def test_get_eligible_customers(
    mocked_get_gocompare_policies,
    mocked_get_gocompare_quotes_between,
    mocked_get_customers_by_id,
):

    eligible_customers = _get_eligible_customers()

    assert set(eligible_customers["customer_uuid"]) == set(
        [
            CUSTOMER1_UUID,
            CUSTOMER2_UUID,
            CUSTOMER3_UUID,
        ]
    )

    assert (
        len(
            eligible_customers[eligible_customers["customer_uuid"] == CUSTOMER1_UUID][
                "pet_name"
            ][0]
        )
        == 2
    )
    assert (
        len(
            eligible_customers[eligible_customers["customer_uuid"] == CUSTOMER2_UUID][
                "pet_name"
            ]
        )
        == 1
    )
    assert (
        len(
            eligible_customers[eligible_customers["customer_uuid"] == CUSTOMER3_UUID][
                "pet_name"
            ]
        )
        == 1
    )
