with
    stripe_customers as (
        select
            cus.stripe_customer_id,
            -- Recover uuid from subscription metadata if missing in customer object
            coalesce(cus.customer_uuid, sub.customer_uuid) as customer_uuid,
            cus.email,
            sub.stripe_subscription_id,
            sub.payment_plan_type,
            sub.created_at as subscription_created_at,
            sub.cancelled_at as subscription_cancelled_at,
            cus.created_at,
            cus.updated_at
        from {{ ref("stg_src_airbyte__stripe_customers") }} cus
        join
            {{ ref("stg_src_airbyte__stripe_subscriptions") }} sub
            on cus.stripe_customer_id = sub.stripe_customer_id
    ),
    booking_db_customers as (
        select
            stripe_customer_id,
            customer_uuid,
            cast(null as string) as email,
            cast(null as string) as stripe_subscription_id,
            cast(null as string) as payment_plan_type,
            cast(null as timestamp) as subscription_created_at,
            cast(null as timestamp) as subscription_cancelled_at,
            created_at,
            updated_at
        from {{ ref("stg_booking_service__customers") }}
    ),
    customers as (
        select *
        from stripe_customers
        union distinct
        select *
        from booking_db_customers
    ),
    -- This backpopulates subscription and customer info for stripe customers that
    -- aren't in our backend (Example: cus_PJWINKZfjrEc8y)
    backpopulate_customer_uuid as (
        select
            stripe_customer_id,
            created_at,
            updated_at,
            first_value(email ignore nulls) over (
                partition by stripe_customer_id order by stripe_subscription_id desc
            ) as email,
            first_value(stripe_subscription_id ignore nulls) over (
                partition by stripe_customer_id order by stripe_subscription_id desc
            ) as stripe_subscription_id,
            first_value(payment_plan_type ignore nulls) over (
                partition by stripe_customer_id order by payment_plan_type desc
            ) as payment_plan_type,
            first_value(subscription_created_at) over (
                partition by stripe_customer_id order by subscription_created_at desc
            ) as subscription_created_at,
            first_value(subscription_cancelled_at) over (
                partition by stripe_customer_id order by subscription_cancelled_at desc
            ) as subscription_cancelled_at,
            coalesce(
                customer_uuid,
                first_value(customer_uuid) over (
                    partition by stripe_customer_id order by customer_uuid desc
                )
            ) as customer_uuid
        from customers
    ),
    -- There should only be 1 row per [customer_uuid, stripe_customer_id] combo
    -- in this table. There can be multiple stripe customers per Napo customer
    training_customers as (
        select * except (_earliest)
        from
            (
                select
                    stripe_customer_id,
                    customer_uuid,
                    email,
                    stripe_subscription_id,
                    payment_plan_type,
                    subscription_created_at,
                    subscription_cancelled_at,
                    created_at,
                    updated_at,
                    min(created_at) over (
                        partition by customer_uuid, stripe_customer_id
                    ) as _earliest
                from backpopulate_customer_uuid
            )
        where created_at = _earliest
    ),
    -- backfill customer email from insurance data if available
    insurance_customer_scd as (
        select *, max(effective_from) over (partition by customer_uuid) as _latest
        from {{ ref("dim_customer") }}
    ),
    insurance_customer_snapshot as (
        select * from insurance_customer_scd where effective_from = _latest
    ),
    final as (
        select
            training.customer_uuid,
            training.stripe_customer_id,
            coalesce(training.email, insurance.email) as email,
            training.stripe_subscription_id,
            training.payment_plan_type,
            training.customer_uuid is not null as is_training_customer,
            insurance.customer_uuid is not null as is_insurance_customer,
            min(cast(training.created_at as date)) over (
                partition by training.customer_uuid
            ) as registration_date,
            subscription_created_at,
            subscription_cancelled_at,
            created_at,
            updated_at
        from training_customers training
        left join
            insurance_customer_snapshot insurance
            on training.customer_uuid = insurance.customer_uuid
    )
select *
from final
