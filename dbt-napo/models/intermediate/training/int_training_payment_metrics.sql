with
    int_payments as (
        select *, count(*) over (partition by customer_uuid) as rn
        from {{ ref("int_training_payments") }}
        where charge_status = 'succeeded' and customer_uuid is not null
    ),
    payment_dates as (
        select
            customer_uuid,
            min(charge_at) over (partition by customer_uuid) as first_payment_at,
            max(charge_at) over (partition by customer_uuid) as last_payment_at
        from int_payments
    ),
    annual_payments as (
        select customer_uuid, 'annually' as payment_plan_type, count(*) as payment_count
        from int_payments
        where payment_plan_type = 'year'
        group by 1
    ),
    monthly_payments as (
        select customer_uuid, 'monthly' as payment_plan_type, count(*) as payment_count
        from int_payments
        where payment_plan_type = 'month'
        group by 1
    ),
    weekly_payments as (
        select customer_uuid, 'weekly' as payment_plan_type, count(*) as payment_count
        from int_payments
        where payment_plan_type = 'week'
        group by 1
    ),
    union_payments as (
        select *
        from monthly_payments
        union all
        select *
        from annual_payments
    ),
    pivot_to_customer_grain as (
        select
            customer_uuid,
            coalesce(annual_payment_count, 0) as annual_payment_count,
            coalesce(monthly_payment_count, 0) as monthly_payment_count,
            coalesce(weekly_payment_count, 0) as weekly_payment_count
        from
            union_payments pivot (
                any_value(payment_count) for payment_plan_type in (
                    'annually' as annual_payment_count,
                    'monthly' as monthly_payment_count,
                    'weekly' as weekly_payment_count
                )
            )
    ),
    final as (
        select distinct
            p1.customer_uuid,
            p1.annual_payment_count,
            p1.monthly_payment_count,
            p1.weekly_payment_count,
            p2.first_payment_at,
            p2.last_payment_at
        from pivot_to_customer_grain p1
        join payment_dates p2 on p1.customer_uuid = p2.customer_uuid
    )
select *
from final
