select
    id as stripe_charge_id,
    paid,
    amount as charge_amount_mu,
    status,
    timestamp_seconds(created) as created_at,
    timestamp_seconds(updated) as updated_at,
    customer as stripe_customer_id,
    json_extract_scalar(outcome, '$.reason') as reason,
    refunded,
    payment_intent as stripe_payment_intent_id,
    failure_message
from {{ source("src_airbyte", "stripe_charges") }}
