with
    stripe_customers as (
        select
            id as stripe_customer_id,
            name,
            email,
            timestamp_seconds(created) as created_at,
            timestamp_seconds(updated) as updated_at,
            max(timestamp_seconds(updated)) over (partition by id) as _latest
        from {{ source("src_airbyte", "stripe_customers") }}
    )
select * except (_latest)
from stripe_customers
where updated_at = _latest
