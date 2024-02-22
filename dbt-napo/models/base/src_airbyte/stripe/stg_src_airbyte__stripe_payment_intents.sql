select
    id as stripe_payment_intent_id,
    customer as stripe_customer_id,
    description as payment_description,
    status as payment_status,
    amount as payment_amount_mu,
    timestamp_seconds(created) as created_at,
    timestamp_seconds(updated) as updated_at
from {{ source("src_airbyte", "stripe_payment_intents") }}
