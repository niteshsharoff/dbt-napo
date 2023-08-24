select
    * except (renewal_at, created_at, updated_at, renewal_quote, id),
    renewal_quote as quote_id,
    timestamp_millis(renewal_at) as renewal_at,
    timestamp_millis(created_at) as created_at,
    timestamp_millis(updated_at) as updated_at,
    timestamp_millis(updated_at) as effective_from,
    lead(timestamp_millis(updated_at), 1, timestamp("2999-01-01 00:00:00+00")) over (
        partition by id order by updated_at
    ) as effective_to,
from {{ source("raw", "renewal") }}
