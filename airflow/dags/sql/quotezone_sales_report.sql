with
    report as (
        select
            date('{{ start_date }}') as start_date,
            date('{{ end_date }}') as end_date,
            cast(date('{{ snapshot_at }}') as timestamp) as snapshot_date,
            'quotezone' as agg_name
    ),
    sold_policy_ids as (
        select distinct (policy_id)
        from raw.policy, report
        where
            quote_source = report.agg_name
            -- is sold
            and (annual_payment_id is not null or subscription_id is not null)
            -- is new policy
            and (
                cast(timestamp_millis(created_date) as date) >= report.start_date
                and cast(timestamp_millis(created_date) as date) < report.end_date
            )
    ),
    cancelled_policy_ids as (
        select distinct (policy_id)
        from raw.policy p, report
        where
            quote_source = report.agg_name
            -- is sold
            and (annual_payment_id is not null or subscription_id is not null)
            -- is cancelled
            and (
                cast(timestamp_millis(cancel_date) as date) >= report.start_date
                and cast(timestamp_millis(cancel_date) as date) < report.end_date
            )
            -- is NTU
            and date_diff(
                cast(timestamp_millis(cancel_date) as date),
                cast(timestamp_millis(p.start_date) as date),
                day
            )
            < 14
    ),
    all_policy_ids as (
        select distinct policy_id
        from
            (
                select policy_id
                from cancelled_policy_ids
                union distinct
                select policy_id
                from sold_policy_ids
            )
    ),
    customer_snapshot as (
        select
            user_id,
            customer_id,
            cast(timestamp_millis(date_of_birth) as date) as date_of_birth,
            postal_code
        from
            (
                select
                    *,
                    timestamp_millis(effective_at) as valid_from,
                    lead(timestamp_millis(effective_at)) over (
                        partition by customer_id order by change_at
                    ) as valid_to
                from raw.customer
            ),
            report
        -- find which customer record to use given a snapshot_date
        where
            (report.snapshot_date >= valid_from and report.snapshot_date < valid_to)
            or (report.snapshot_date >= valid_from and valid_to is null)
    ),
    customer_info as (
        select
            c.customer_id,
            u.first_name,
            u.last_name,
            c.date_of_birth,
            c.postal_code,
            u.email
        from customer_snapshot c
        join raw.user u on c.user_id = u.id
    ),
    policy_snapshot as (
        select *
        from
            (
                select
                    policy_id,
                    customer_id,
                    quote_id,
                    cast(timestamp_millis(created_date) as date) as quote_sale_date,
                    cast(timestamp_millis(start_date) as date) as incept_date,
                    cast(timestamp_millis(cancel_date) as date) as cancel_date,
                    annual_price as price_paid,
                    (cancel_date is not null)
                    and (
                        annual_payment_id is not null or subscription_id is not null
                    ) as cancelled,
                    -- Use policy ledger to find the correct policy record based on
                    -- the effective_at timestamp
                    timestamp_millis(effective_at) as _valid_from,
                    lead(timestamp_millis(effective_at)) over (
                        partition by policy_id order by change_at
                    ) as _valid_to,
                    reference_number,
                    run_date
                from raw.policy
                where policy_id in (select policy_id from all_policy_ids)
            ),
            report
        where
            (report.snapshot_date >= _valid_from and report.snapshot_date < _valid_to)
            or (report.snapshot_date >= _valid_from and _valid_to is null)
    ),
    quotezone_report as (
        select
            '3938' as `Brand ID`,
            c.first_name as `First Name`,
            c.last_name as `Surname`,
            format_date("%d-%m-%Y", c.date_of_birth) as `Date of Birth`,
            c.postal_code as `Postcode`,
            c.email as `Email`,
            format_date(
                "%d-%m-%Y", cast(timestamp_millis(q.created_at) as date)
            ) as `Quote Date`,
            format_date("%d-%m-%Y", p.incept_date) as `Incept Date`,
            format_date("%d-%m-%Y", p.quote_sale_date) as `Quote Sale Date`,
            '' as `Single Combined`,
            p.price_paid as `Price Paid`,
            case when p.cancelled is true then 'Y' else '' end as `Cancelled`
        -- , p.policy_id as _policy_id
        from policy_snapshot p
        join customer_info c on p.customer_id = c.customer_id
        join raw.quoterequest q on p.quote_id = q.quote_request_id
    )
select *
from quotezone_report
;
