with
    raw as (
        select
            * except (customer_id),
            row_number() over (
                partition by customer_uuid order by run_date desc
            ) as row_no
        from {{ ref("stg_raw__booking_service_customer") }}
        qualify row_no = 1
    )
select * except (row_no)
from raw
