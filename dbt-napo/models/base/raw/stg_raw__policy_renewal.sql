select id as renewal_id, * except (id) from {{ source("raw", "renewal") }}
