import uuid
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from pytest_mock import MockerFixture

CUSTOMER1_UUID = str(uuid.uuid4())
CUSTOMER2_UUID = str(uuid.uuid4())
CUSTOMER3_UUID = str(uuid.uuid4())
CUSTOMER1_POLICY1_QUOTE_ID = str(uuid.uuid4())
CUSTOMER1_POLICY2_QUOTE_ID = str(uuid.uuid4())
CUSTOMER2_POLICY1_QUOTE_ID = str(uuid.uuid4())
CUSTOMER3_POLICY1_QUOTE_ID = str(uuid.uuid4())


@pytest.fixture()
def mocked_get_gocompare_policies(mocker: MockerFixture):
    customer1_policy1 = dict(
        customer_uuid=CUSTOMER1_UUID,
        quote_id=CUSTOMER1_POLICY1_QUOTE_ID,
        quote_source="gocompare",
        policy_start_date=datetime(2024, 1, 2),
        name="Fido",
    )
    customer1_policy2 = dict(
        customer_uuid=CUSTOMER1_UUID,
        quote_id=CUSTOMER1_POLICY1_QUOTE_ID,
        quote_source="gocompare",
        policy_start_date=datetime(2024, 1, 2),
        name="Rover",
    )
    customer2_policy1 = dict(
        customer_uuid=CUSTOMER2_UUID,
        quote_id=CUSTOMER2_POLICY1_QUOTE_ID,
        quote_source="gocompare",
        policy_start_date=datetime(2024, 1, 2),
        name="Rex",
    )
    customer3_policy1 = dict(
        customer_uuid=CUSTOMER3_UUID,
        quote_id=CUSTOMER3_POLICY1_QUOTE_ID,
        quote_source="gocompare",
        policy_start_date=datetime(2024, 1, 2),
        name="Bob",
    )
    gocompare_policies = pd.DataFrame(
        np.array(
            [
                [
                    customer1_policy1["customer_uuid"],
                    customer1_policy1["quote_id"],
                    customer1_policy1["quote_source"],
                    customer1_policy1["policy_start_date"],
                    customer1_policy1["name"],
                ],
                [
                    customer1_policy2["customer_uuid"],
                    customer1_policy2["quote_id"],
                    customer1_policy2["quote_source"],
                    customer1_policy2["policy_start_date"],
                    customer1_policy2["name"],
                ],
                [
                    customer2_policy1["customer_uuid"],
                    customer2_policy1["quote_id"],
                    customer2_policy1["quote_source"],
                    customer2_policy1["policy_start_date"],
                    customer2_policy1["name"],
                ],
                [
                    customer3_policy1["customer_uuid"],
                    customer3_policy1["quote_id"],
                    customer3_policy1["quote_source"],
                    customer3_policy1["policy_start_date"],
                    customer3_policy1["name"],
                ],
            ]
        ),
        columns=[
            "customer_uuid",
            "quote_uuid",
            "quote_source",
            "policy_start_date",
            "name",
        ],
    )
    gocompare_policies = gocompare_policies.convert_dtypes()

    mocked = mocker.patch("dags.reward_go_compare._get_gocompare_policies")
    mocked.return_value = gocompare_policies

    return mocked


@pytest.fixture()
def mocked_get_gocompare_quotes_between(mocker: MockerFixture):
    gocompare_quotes_between = pd.DataFrame(
        np.array(
            [
                [
                    CUSTOMER1_POLICY1_QUOTE_ID,
                    "gocompare",
                    datetime(2024, 1, 1).isoformat(),
                    {"start_date": datetime(2024, 1, 1)},
                ],
                [
                    CUSTOMER2_POLICY1_QUOTE_ID,
                    "gocompare",
                    datetime(2024, 1, 1).isoformat(),
                    {"start_date": datetime(2024, 1, 1)},
                ],
                [
                    CUSTOMER3_POLICY1_QUOTE_ID,
                    "gocompare",
                    datetime(2024, 1, 1).isoformat(),
                    {"start_date": datetime(2024, 1, 1)},
                ],
            ]
        ),
        columns=["quote_uuid", "quote_source", "quote_at", "common_quote"],
    )

    gocompare_quotes_between = gocompare_quotes_between.convert_dtypes()

    mocked = mocker.patch("dags.reward_go_compare._get_gocompare_quotes_between")
    mocked.return_value = gocompare_quotes_between


@pytest.fixture()
def mocked_get_customers_by_id(mocker: MockerFixture):
    get_customers_by_id = pd.DataFrame(
        np.array(
            [
                [CUSTOMER1_UUID, "linda@test.com", "Linda"],
                [CUSTOMER2_UUID, "sam@test.com", "Sam"],
                [CUSTOMER3_UUID, "john@test.com", "John"],
            ]
        ),
        columns=["customer_uuid", "email", "first_name"],
    )

    get_customers_by_id = get_customers_by_id.convert_dtypes()

    mocked = mocker.patch("dags.reward_go_compare._get_customers_by_id")
    mocked.return_value = get_customers_by_id
    return mocked
