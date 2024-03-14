{{ config(schema="marts", tags=["daily"]) }}

with
    events as (
        select *, date(event_tx) as date from {{ ref("int_training_payment_events") }}
    ),
    ad_spend as (
        select 'ad_spend' as metric, date, sum(total_spend) as cnt
        from {{ ref("growth_sales_by_channel") }}
        where channel = 'pa_standalone'
        group by date
    ),
    registrations as (
        select 'registrations' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where event_type = 'pa_registration'
        group by date
    ),
    free_trials_started as (
        select
            'free_trials_started' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where event_type = 'trial_started'
        group by date
    ),
    new_annual as (
        select 'new_annual' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where event_type = 'payment' and payment_plan_type = 'year'
        group by date
    ),
    new_monthly as (
        select 'new_monthly' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where
            event_type = 'payment'
            and payment_plan_type = 'month'
            and recurring_payment = false
        group by date
    ),
    new_weekly as (
        select 'new_weekly' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where
            event_type = 'payment'
            and payment_plan_type = 'week'
            and recurring_payment = false
        group by date
    ),
    recurring_monthly as (
        select 'recurring_monthly' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where
            event_type = 'payment'
            and payment_plan_type = 'month'
            and recurring_payment = true
        group by date
    ),
    recurring_weekly as (
        select 'recurring_weekly' as metric, date, count(distinct customer_uuid) as cnt
        from events
        where
            event_type = 'payment'
            and payment_plan_type = 'week'
            and recurring_payment = true
        group by date
    ),
    new_revenue as (
        select 'new_revenue' as metric, date, sum(payment_amount) as cnt
        from events
        where event_type = 'payment' and recurring_payment = false
        group by date
    ),
    recurring_revenue as (
        select 'recurring_revenue' as metric, date, sum(payment_amount) as cnt
        from events
        where event_type = 'payment' and recurring_payment = true
        group by date
    ),
    other_revenue as (
        select 'other_revenue' as metric, date, sum(payment_amount) as cnt
        from events
        where event_type = 'payment' and recurring_payment is null
        group by date
    ),
    refunds as (
        select 'refund' as metric, date, sum(refund_amount) as cnt
        from events
        where event_type = 'refund'
        group by date
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
        from new_weekly
        union all
        select *
        from recurring_monthly
        union all
        select *
        from recurring_weekly
        union all
        select *
        from new_revenue
        union all
        select *
        from recurring_revenue
        union all
        select *
        from other_revenue
        union all
        select *
        from refunds
    ),
    metrics_table as (
        select
            date,
            cast(coalesce(round(ad_spend, 2), 0.0) as numeric) as ad_spend,
            coalesce(cast(registrations as int), 0) as registrations,
            coalesce(cast(free_trials_started as int), 0) as free_trials_started,
            coalesce(cast(new_annual as int), 0) as new_annual,
            coalesce(cast(new_monthly as int), 0) as new_monthly,
            coalesce(cast(new_weekly as int), 0) as new_weekly,
            coalesce(cast(recurring_monthly as int), 0) as recurring_monthly,
            coalesce(cast(recurring_weekly as int), 0) as recurring_weekly,
            cast(coalesce(round(new_revenue, 2), 0.0) as numeric) as new_revenue,
            cast(
                coalesce(round(recurring_revenue, 2), 0.0) as numeric
            ) as recurring_revenue,
            cast(coalesce(round(other_revenue, 2), 0.0) as numeric) as other_revenue,
            cast(coalesce(round(refund, 2), 0.0) as numeric) as refund_amount
        from
            final pivot (
                sum(cnt) for metric in (
                    'ad_spend',
                    'registrations',
                    'free_trials_started',
                    'new_annual',
                    'new_monthly',
                    'new_weekly',
                    'recurring_monthly',
                    'recurring_weekly',
                    'new_revenue',
                    'recurring_revenue',
                    'other_revenue',
                    'refund'
                )
            )
        order by date
    )
select *
from metrics_table
