with
    refunds as (
        select
            id as stripe_refund_id,
            payment_intent as stripe_payment_intent_id,
            amount as refund_amount_mu,
            reason as refund_reason,
            status as refund_status,
            timestamp_seconds(created) as refunded_at
        from {{ source("src_airbyte", "stripe_refunds") }}
    )
select *
from refunds
