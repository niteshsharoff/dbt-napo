with raw as (
    select 
        userId as user_id
        ,message
        ,_imported_at
    from {{source('firestore','sms_reminders')}}
)
select * 
from raw