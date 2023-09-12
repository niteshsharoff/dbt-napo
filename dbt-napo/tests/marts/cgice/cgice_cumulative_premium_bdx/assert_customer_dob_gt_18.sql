{{ config(severity="warn") }}
/*
  GIVEN
    a table of policy transactions

  WHEN
    we get the latest transaction for each active policy

  THEN
    we expect the customer's DOB to be greater than 18
*/
with
    latest_transactions as (
        select
            transaction_type,
            transaction_at,
            policy_number,
            customer_dob,
            start_date,
            end_date,
            transaction_at
            = max(transaction_at) over (partition by policy_number) as _include
        from {{ ref("cgice_cumulative_premium_bdx") }}
    ),
    discrepancies as (
        select
            extract(year from transaction_at) as report_year,
            extract(month from transaction_at) as report_month,
            * except (_include)
        from latest_transactions
        where _include = true and date_diff(start_date, customer_dob, year) < 18
        order by report_year desc, report_month desc
    ),
    filtered_discrepancies as (
        select *
        from discrepancies
        where
            report_year
            = extract(year from parse_date('%Y-%m-%d', '{{run_started_at.date()}}'))
            -- 2 month window for monitoring issues
            and (
                report_month = extract(
                    month from parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
                )
                or report_month = extract(
                    month from parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
                )
                - 1
            )
    )
select *
from filtered_discrepancies
