with
    raw as (
        select id, userid as user_id, message, _imported_at
        from {{ source("firestore", "email_reminders") }}
    )
select *
from raw
