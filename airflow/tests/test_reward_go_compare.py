from dags.reward_go_compare import _get_eligible_customers
from tests.make import *


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
