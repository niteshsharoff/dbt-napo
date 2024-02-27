with
    raw as (
        select
            * except (
                created_date,
                age_months,  -- deprecated column, don't use!
                date_of_birth,
                change_at,
                effective_at,
                customer_id
            ),
            cast(timestamp_millis(date_of_birth) as date) as date_of_birth,
            cast(timestamp_millis(created_date) as date) as created_date,
            timestamp_millis(change_at) as change_at,
            cast(timestamp_millis(effective_at) as date) as effective_at,
            row_number() over (partition by pet_id order by version_id desc) as row_no
        from {{ source("raw", "pet") }}
    )

select * except (row_no)
from raw
where row_no = 1
