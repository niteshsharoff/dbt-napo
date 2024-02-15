with
    registrations as (
        select
            created_at as transaction_at,
            'pa_registration' as transaction_type,
            customer_uuid,
            stripe_customer_id,
            cast(null as string) as stripe_subscription_id,
            cast(null as string) as payment_plan_type,
            null as payment_amount,
            cast(null as string) as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_raw__booking_service_customer") }}
        where customer_uuid is not null
    ),
    trial_started as (
        select
            trial_started_at as transaction_at,
            'trial_started' as trial_started_at,
            customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            null as payment_amount,
            cast(null as string) as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_subscriptions") }}
        where trial_started_at is not null
    ),
    trial_ended as (
        select
            trial_ended_at as transaction_at,
            'trial_ended' as transaction_type,
            customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            null as payment_amount,
            cast(null as string) as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_subscriptions") }}
        where trial_ended_at is not null
    ),
    cancellations as (
        select
            cancelled_at as transaction_at,
            'cancelled' as transaction_type,
            customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            null as payment_amount,
            cast(null as string) as notes,
            cancellation_reason as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_subscriptions") }}
        where cancelled_at is not null
    ),
    payment_intents as (
        select
            pi.created_at as transaction_at,
            'payment_intent' as transaction_type,
            cu.customer_uuid as customer_uuid,
            pi.stripe_customer_id,
            cu.stripe_subscription_id,
            cu.payment_plan_type,
            pi.payment_amount_mu / 100 as payment_amount,
            pi.payment_description as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_payment_intents") }} pi
        left join
            {{ ref("int_training_customers") }} cu
            on pi.stripe_customer_id = cu.stripe_customer_id
    ),
    events as (
        select *
        from registrations
        union distinct
        select *
        from trial_started
        union distinct
        select *
        from trial_ended
        union distinct
        select *
        from cancellations
        union distinct
        select *
        from payment_intents
    ),
    recurring_payments as (
        select
            * except (rn),
            case
                when
                    transaction_type = 'payment_intent'
                    and rn > 1
                    -- There is currently no reliable way to discern other payments.
                    -- AFAIK these are payments where the customer_uuid isn't tracked
                    -- in our internal systems.
                    and customer_uuid is not null
                then true
                when
                    transaction_type = 'payment_intent'
                    and rn = 1
                    and customer_uuid is not null
                then false
                else null
            end as recurring_payment
        from
            (
                select
                    *,
                    -- Partition by subscription_id to so we don't count miscount
                    -- first payments on renewal
                    -- Partition by transaction type to get the instalment number for
                    -- a subscription
                    row_number() over (
                        partition by
                            stripe_customer_id, transaction_type, stripe_subscription_id
                        order by transaction_at
                    ) as rn
                from events
            )
    )
select *
from recurring_payments
