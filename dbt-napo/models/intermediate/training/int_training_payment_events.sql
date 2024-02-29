with
    registrations as (
        select
            created_at as event_tx,
            'pa_registration' as event_type,
            customer_uuid,
            stripe_customer_id,
            cast(null as string) as stripe_subscription_id,
            cast(null as string) as payment_plan_type,
            0.0 as payment_amount,
            0.0 as refund_amount,
            cast(null as string) as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_booking_service__customers") }}
        where customer_uuid is not null
    ),
    trial_started as (
        select
            trial_started_at as event_tx,
            'trial_started' as trial_started_at,
            customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            0.0 as payment_amount,
            0.0 as refund_amount,
            cast(null as string) as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_subscriptions") }}
        where trial_started_at is not null
    ),
    trial_ended as (
        select
            trial_ended_at as event_tx,
            'trial_ended' as event_type,
            customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            0.0 as payment_amount,
            0.0 as refund_amount,
            cast(null as string) as notes,
            cast(null as string) as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_subscriptions") }}
        where trial_ended_at is not null
    ),
    cancellations as (
        select
            cancelled_at as event_tx,
            'cancelled' as event_type,
            customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            0.0 as payment_amount,
            0.0 as refund_amount,
            cast(null as string) as notes,
            cancellation_reason as cancel_reason
        from {{ ref("stg_src_airbyte__stripe_subscriptions") }}
        where cancelled_at is not null
    ),
    payments as (
        select
            coalesce(charge_at, payment_intent_at) as event_tx,
            'payment' as event_type,
            customer_uuid as customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            charge_amount as payment_amount,
            0.0 as refund_amount,
            failure_reason as notes,
            cast(null as string) as cancel_reason
        from {{ ref("int_training_payments") }}
        where charge_status = 'succeeded'
    ),
    refunds as (
        select
            coalesce(refund_at) as event_tx,
            'refund' as event_type,
            customer_uuid as customer_uuid,
            stripe_customer_id,
            stripe_subscription_id,
            payment_plan_type,
            0.0 as payment_amount,
            refund_amount,
            refund_reason as notes,
            cast(null as string) as cancel_reason
        from {{ ref("int_training_payments") }}
        where refund_status = 'succeeded'
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
        from payments
        union distinct
        select *
        from refunds
    ),
    recurring_payments as (
        select
            *,
            case
                when
                    event_type = 'payment'
                    and rn > 1
                    -- There is currently no reliable way to discern other payments.
                    -- AFAIK these are payments where the customer_uuid isn't tracked
                    -- in our internal systems.
                    and customer_uuid is not null
                then true
                when event_type = 'payment' and rn = 1 and customer_uuid is not null
                then false
                else null
            end as recurring_payment
        from
            (
                select
                    *,
                    -- Partition by subscription_id so we don't count miscount
                    -- first payments on renewal
                    -- Partition by transaction type to get the instalment number for
                    -- a subscription
                    row_number() over (
                        partition by
                            stripe_customer_id, event_type, stripe_subscription_id
                        order by event_tx
                    ) as rn
                from events
            )
    )
select distinct
    py.event_tx,
    py.event_type,
    py.customer_uuid,
    py.stripe_customer_id,
    py.stripe_subscription_id,
    py.payment_plan_type,
    py.payment_amount,
    py.recurring_payment,
    py.refund_amount,
    py.notes,
    py.cancel_reason
from recurring_payments py
left join {{ ref("int_training_customers") }} cu on py.customer_uuid = cu.customer_uuid
