select distinct
    payment_intent.payment_description as description,
    charge.status as charge_status,
    charge.charge_amount_mu / 100 as charge_amount,
    charge.reason as failure_reason,
    subscription.payment_plan_type,
    refund.refund_status,
    refund.refund_amount_mu / 100 as refund_amount,
    refund.refund_reason,
    charge.receipt_email as receipt_email,
    subscription.customer_uuid as customer_uuid,
    payment_intent.created_at as payment_intent_at,
    charge.created_at as charge_at,
    refund.refunded_at as refund_at,
    payment_intent.stripe_payment_intent_id,
    charge.stripe_charge_id,
    payment_intent.stripe_customer_id,
    subscription.stripe_subscription_id
from {{ ref("stg_src_airbyte__stripe_payment_intents") }} payment_intent
left join
    {{ ref("stg_src_airbyte__stripe_charges") }} charge
    on payment_intent.stripe_payment_intent_id = charge.stripe_payment_intent_id
left join
    {{ ref("stg_src_airbyte__stripe_subscriptions") }} subscription
    on payment_intent.stripe_customer_id = subscription.stripe_customer_id
left join
    {{ ref("stg_src_airbyte__stripe_refunds") }} refund
    on payment_intent.stripe_payment_intent_id = refund.stripe_payment_intent_id
