/*
  GIVEN
    a table of customer transactions
  WHEN
    we search for overlapping effective_from and effective_to timestamps
  THEN
    we expect no transactions with overlapping time ranges
*/
with
    overlapping_date_range as (
        select *
        from
            (
                select
                    customer_uuid,
                    effective_from,
                    effective_to,
                    lag(effective_to, 1) over (
                        partition by customer_uuid order by effective_from
                    ) prev_effective_to,
                    lead(effective_from, 1) over (
                        partition by customer_uuid order by effective_from
                    ) next_effective_from
                from {{ ref("dim_customer") }}
            ) q1
        where
            (prev_effective_to is not null and prev_effective_to > next_effective_from)
            or (
                next_effective_from is not null
                and next_effective_from < prev_effective_to
            )
    )
select *
from overlapping_date_range
