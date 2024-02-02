{{ config(materialized="table") }}

with
    pet_ledger as (
        select
            * except (created_date, date_of_birth, change_at, effective_at),
            extract(date from timestamp_millis(created_date)) as created_date,
            extract(date from timestamp_millis(date_of_birth)) as date_of_birth,
            timestamp_millis(change_at) as change_at,
            timestamp_millis(effective_at) as effective_from,
            lead(
                timestamp_millis(effective_at), 1, timestamp("2999-01-01 00:00:00+00")
            ) over (partition by pet_id order by effective_at) as effective_to,
            max(version_id) over (
                partition by pet_id order by version_id desc
            ) as latest_version
        from {{ source("raw", "pet") }}
    )
-- The age_months column from Django is wrong, we should calculate pet age using DOB
-- instead
select * except (age_months)
from pet_ledger
