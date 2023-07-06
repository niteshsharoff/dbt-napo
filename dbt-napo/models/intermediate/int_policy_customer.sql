select
    p.version_id,
    p.policy_id,
    p.quote_id,
    p.reference_number,
    -- ,p.subscription_active
    p.is_subscription_active,
    p.annual_payment_id,
    -- ,p.active_subscription_existed
    p.active_subscription_setup,
    p.created_date,
    p.policy_effective_date,
    p.last_subscription_created_date,
    p.last_subscription_modified_date,
    p.start_date as policy_start_date,
    p.end_date as policy_end_date,
    p.cancel_date as policy_cancel_date,
    p.cancel_reason as policy_cancel_reason,
    p.renewal_approved,
    p.payment_plan_type,
    p.monthly_price,
    p.annual_price,
    p.customer_id,
    c.uuid as customer_uuid,
    p.pet_id,
    p.product_id,
    p.quote_source_reference,
    p.quote_source,
    p.voucher_code_id,
    p.first_payment_charge_date,
    p.last_payment_charge_date,
    c.year_of_birth,
    (
        extract(year from current_date()) - cast(c.year_of_birth as numeric)
    ) as customer_age,
    c.user_id
from {{ ref("int_policy_subscription") }} p
left join {{ ref("stg_raw__customer") }} c on p.customer_id = c.customer_id
