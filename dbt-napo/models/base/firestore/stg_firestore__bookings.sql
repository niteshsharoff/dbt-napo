with
    raw as (
        select
            id,
            userid as user_id,
            sessionid as session_id,
            parse_datetime('%Y-%m-%d %H:%M', date) as class_date,
            cancelled,
            attendance,
            classid as class_id,
            classname as class_name,
            classdescription as class_description,
            notify,
            _imported_at

        from {{ source("firestore", "bookings") }}
    )
select *
from raw
