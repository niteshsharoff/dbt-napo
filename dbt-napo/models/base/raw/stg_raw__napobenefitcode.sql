select
    * except (created_date, expires_after),
    timestamp_millis(created_date) as created_date,
    timestamp_millis(expires_after) as expires_after
from {{ source("raw", "napobenefitcode") }}
where run_date = (select max(run_date) from {{ source("raw", "napobenefitcode") }})
