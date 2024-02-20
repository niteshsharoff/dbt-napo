with
    stripe_customers as (
        select
            cus.stripe_customer_id,
            -- Recover uuid from subscription metadata if missing in customer object
            coalesce(cus.customer_uuid, sub.customer_uuid) as customer_uuid,
            sub.stripe_subscription_id,
            sub.payment_plan_type,
            cus.created_at,
            cus.updated_at
        from {{ ref("stg_src_airbyte__stripe_customer") }} cus
        join
            {{ ref("stg_src_airbyte__stripe_subscription") }} sub
            on cus.stripe_customer_id = sub.stripe_customer_id
    ),
    booking_db_customers as (
        select
            stripe_customer_id,
            customer_uuid,
            cast(null as string) as stripe_subscription_id,
            cast(null as string) as payment_plan_type,
            created_at,
            updated_at
        from {{ ref("stg_raw__booking_service_customer") }}
    ),
    customers as (
        select *
        from stripe_customers
        union distinct
        select *
        from booking_db_customers
    ),
    -- This backpopulates subscription and customer info for stripe customers that
    -- aren't in our
    -- backend (Example: cus_PJWINKZfjrEc8y)
    backpopulate_customer_uuid as (
        select
            * except (customer_uuid, payment_plan_type, stripe_subscription_id),
            first_value(stripe_subscription_id ignore nulls) over (
                partition by stripe_customer_id order by stripe_subscription_id desc
            ) as stripe_subscription_id,
            first_value(payment_plan_type ignore nulls) over (
                partition by stripe_customer_id order by payment_plan_type desc
            ) as payment_plan_type,
            first_value(customer_uuid ignore nulls) over (
                partition by stripe_customer_id order by customer_uuid desc
            ) as customer_uuid
        from customers
    ),
    -- There should only be 1 row per [customer_uuid, stripe_customer_id] combo
    -- in this table. There can be multiple stripe customers per Napo customer
    final as (
        select
            *,
            min(created_at) over (
                partition by customer_uuid, stripe_customer_id
            ) as _earliest
        from backpopulate_customer_uuid
    )
select *
from final
where created_at = _earliest
