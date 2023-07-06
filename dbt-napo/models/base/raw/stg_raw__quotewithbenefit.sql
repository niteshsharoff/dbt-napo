select * except (created_date), timestamp_millis(created_date) as created_date
from {{ source("raw", "quotewithbenefit") }}
