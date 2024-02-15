with
    transactions as (
        select *, date(transaction_at) as transaction_date
        from {{ ref("int_training_transactions") }}
    ),
    ad_spend as (
        select 'ad_spend' as metric, date as transaction_date, sum(total_spend) as cnt
        from {{ ref("growth_sales_by_channel") }}
        where channel = 'pa_standalone'
        group by date
    ),
    registrations as (
        select
            'registrations' as metric,
            transaction_date,
            count(distinct customer_uuid) as cnt
        from transactions
        where transaction_type = 'pa_registration'
        group by transaction_date
    ),
    free_trials_started as (
        select
            'free_trials_started' as metric,
            transaction_date,
            count(distinct customer_uuid) as cnt
        from transactions
        where transaction_type = 'trial_started'
        group by transaction_date
    ),
    new_annual as (
        select
            'new_annual' as metric,
            transaction_date,
            count(distinct customer_uuid) as cnt
        from transactions
        where transaction_type = 'payment_intent' and payment_plan_type = 'year'
        group by transaction_date
    ),
    new_monthly as (
        select
            'new_monthly' as metric,
            transaction_date,
            count(distinct customer_uuid) as cnt
        from transactions
        where
            transaction_type = 'payment_intent'
            and payment_plan_type = 'month'
            and recurring_payment = false
        group by transaction_date
    ),
    recurring_monthly as (
        select
            'recurring_monthly' as metric,
            transaction_date,
            count(distinct customer_uuid) as cnt
        from transactions
        where
            transaction_type = 'payment_intent'
            and payment_plan_type = 'month'
            and recurring_payment = true
        group by transaction_date
    ),
    new_revenue as (
        select 'new_revenue' as metric, transaction_date, sum(payment_amount) as cnt
        from transactions
        where
            transaction_type = 'payment_intent'
            and customer_uuid is not null
            and recurring_payment = false
        group by transaction_date
    ),
    recurring_revenue as (
        select
            'recurring_revenue' as metric, transaction_date, sum(payment_amount) as cnt
        from transactions
        where
            transaction_type = 'payment_intent'
            and customer_uuid is not null
            and recurring_payment = true
        group by transaction_date
    ),
    other_revenue as (
        select 'other_revenue' as metric, transaction_date, sum(payment_amount) as cnt
        from transactions
        where
            transaction_type = 'payment_intent'
            and customer_uuid is null
            and stripe_customer_id is null
        group by transaction_date
    ),
    final as (
        select *
        from ad_spend
        union all
        select *
        from registrations
        union all
        select *
        from free_trials_started
        union all
        select *
        from new_annual
        union all
        select *
        from new_monthly
        union all
        select *
        from recurring_monthly
        union all
        select *
        from new_revenue
        union all
        select *
        from recurring_revenue
        union all
        select *
        from other_revenue
    ),
    metrics_table as (
        select
            transaction_date,
            cast(coalesce(round(ad_spend, 2), 0.0) as numeric) as ad_spend,
            coalesce(cast(registrations as int), 0) as registrations,
            coalesce(cast(free_trials_started as int), 0) as free_trials_started,
            coalesce(cast(new_annual as int), 0) as new_annual,
            coalesce(cast(new_monthly as int), 0) as new_monthly,
            coalesce(cast(recurring_monthly as int), 0) as recurring_monthly,
            cast(coalesce(round(new_revenue, 2), 0.0) as numeric) as new_revenue,
            cast(
                coalesce(round(recurring_revenue, 2), 0.0) as numeric
            ) as recurring_revenue,
            cast(coalesce(round(other_revenue, 2), 0.0) as numeric) as other_revenue
        from
            final pivot (
                sum(cnt) for metric in (
                    'ad_spend',
                    'registrations',
                    'free_trials_started',
                    'new_annual',
                    'new_monthly',
                    'recurring_monthly',
                    'new_revenue',
                    'recurring_revenue',
                    'other_revenue'
                )
            )
        order by transaction_date
    )
select *
from metrics_table
