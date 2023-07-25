{{ config(severity="warn") }}
/*
  GIVEN
    a table of policy transactions

  WHEN
    we get the latest transaction for each policy

  THEN
    we expect customer's postal code to be valid
*/
with
    latest_transactions as (
        select
            transaction_type,
            transaction_at,
            policy_number,
            customer_postal_code,
            end_date,
            transaction_at
            = max(transaction_at) over (partition by policy_number) as _include
        from {{ ref("cgice_cumulative_premium_bdx") }}
    ),
    discrepancies as (
        select
            extract(year from transaction_at) as report_year,
            extract(month from transaction_at) as report_month,
            * except (end_date, _include)
        from latest_transactions
        where
            _include = true
            -- https://stackoverflow.com/questions/164979/regex-for-matching-uk-postcodes
            and not regexp_contains(
                customer_postal_code,
                r'^([A-Za-z][A-Ha-hJ-Yj-y]?[0-9][A-Za-z0-9]? ?[0-9][A-Za-z]{2}|[Gg][Ii][Rr] ?0[Aa]{2})$'
            )
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
