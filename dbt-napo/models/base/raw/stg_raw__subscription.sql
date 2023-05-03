select * except(created_date,modified_date)
        ,date(timestamp_millis(created_date)) as created_date
        ,date(timestamp_millis(modified_date)) as modified_date
from {{source('raw','subscription')}}