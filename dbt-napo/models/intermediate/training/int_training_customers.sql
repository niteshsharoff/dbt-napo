with
    -- All customers in our Insurance databases, including those who haven't purchased
    -- a policy
    dim_customer as (
        select customer_uuid, email
        from
            (
                select
                    *, max(effective_from) over (partition by customer_uuid) as _latest
                from {{ ref("dim_customer") }}
            )
        where effective_from = _latest
    ),
    -- Recover uuid from subscription metadata if missing in customer object
    stripe_customers as (
        select cus.stripe_customer_id, sub.customer_uuid as customer_uuid, cus.email
        from {{ ref("stg_src_airbyte__stripe_customers") }} cus
        left join
            {{ ref("stg_src_airbyte__stripe_subscriptions") }} sub
            on cus.stripe_customer_id = sub.stripe_customer_id
    ),
    booking_db_customers as (
        select stripe_customer_id, customer_uuid
        from {{ ref("stg_booking_service__customers") }}
    ),
    booking_and_stripe_customers as (
        select
            -- Backpopulate customer_uuid for customer data pulled from stripe
            coalesce(booking.customer_uuid, stripe.customer_uuid) as customer_uuid,
            stripe.email
        from booking_db_customers booking
        full outer join
            stripe_customers stripe
            on booking.stripe_customer_id = stripe.stripe_customer_id
    ),
    training_customers as (
        select
            training.customer_uuid,
            -- Backpopulate email address for training customers
            coalesce(training.email, dim_customer.email) as email
        from booking_and_stripe_customers training
        join dim_customer on training.customer_uuid = dim_customer.customer_uuid
    ),
    -- The grain of this table is at the policy level, this captures only customers
    -- that have purchased a policy
    insurance_customers as (
        select customer.customer_uuid, customer.email
        from {{ ref("dim_policy_claim_snapshot") }}
        where
            snapshot_date
            = (select max(snapshot_date) from {{ ref("dim_policy_claim_snapshot") }})
    ),
    insurance_and_training_customers as (
        select distinct
            insurance.customer_uuid as insurance_customer_uuid,
            -- backpopulate email addresses for insurance customers
            coalesce(insurance.email, training.email) as email,
            training.customer_uuid as training_customer_uuid,
            case
                when insurance.customer_uuid is not null then true else false
            end as is_insurance_customer,
            case
                when training.customer_uuid is not null then true else false
            end as is_training_customer
        from insurance_customers insurance
        full outer join
            training_customers training
            on insurance.customer_uuid = training.customer_uuid
    ),
    one_off_stripe_payments as (
        select distinct
            dim_customer.customer_uuid as customer_uuid, receipt_email as email
        from {{ ref("int_training_payments") }} payment
        left join dim_customer on payment.receipt_email = dim_customer.email
        where payment.customer_uuid is null
    ),
    all_customers as (
        select distinct
            coalesce(
                insurance_customer_uuid, training_customer_uuid, pmt.customer_uuid
            ) as customer_uuid,
            coalesce(cus.email, pmt.email) as email,
            coalesce(is_insurance_customer, false) as is_insurance_customer,
            case
                when pmt.email is not null then true else is_training_customer
            end as is_training_customer
        from insurance_and_training_customers cus
        full outer join one_off_stripe_payments pmt on cus.email = pmt.email
    ),
    -- Lookup registration date and subscription dates
    final as (
        select
            customer.customer_uuid,
            customer.email,
            cast(backend.created_at as date) as registration_date,
            subscription.stripe_customer_id,
            subscription.created_at as subscription_created_at,
            subscription.cancelled_at as subscription_cancelled_at,
            customer.is_insurance_customer,
            customer.is_training_customer
        from all_customers customer
        left join
            {{ ref("stg_booking_service__customers") }} backend
            on customer.customer_uuid = backend.customer_uuid
        left join
            {{ ref("stg_src_airbyte__stripe_subscriptions") }} subscription
            on customer.customer_uuid = subscription.customer_uuid
    )
select *
from final
where is_training_customer = true
