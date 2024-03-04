select distinct
    pi.payment_description as description,
    ch.status as charge_status,
    ch.charge_amount_mu / 100 as charge_amount,
    ch.reason as failure_reason,
    cu.payment_plan_type,
    re.refund_status,
    re.refund_amount_mu / 100 as refund_amount,
    re.refund_reason,
    coalesce(cu.email, ch.receipt_email) as email,
    cu.customer_uuid as customer_uuid,
    pi.created_at as payment_intent_at,
    ch.created_at as charge_at,
    re.refunded_at as refund_at,
    pi.stripe_payment_intent_id,
    ch.stripe_charge_id,
    pi.stripe_customer_id,
    cu.stripe_subscription_id
from {{ ref("stg_src_airbyte__stripe_payment_intents") }} pi
left join
    {{ ref("stg_src_airbyte__stripe_charges") }} ch
    on pi.stripe_payment_intent_id = ch.stripe_payment_intent_id
left join
    {{ ref("int_training_customers") }} cu
    on pi.stripe_customer_id = cu.stripe_customer_id
left join
    {{ ref("stg_src_airbyte__stripe_refunds") }} re
    on pi.stripe_payment_intent_id = re.stripe_payment_intent_id
