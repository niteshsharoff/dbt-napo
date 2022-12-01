with raw as (
    select 
         userId as user_id
        ,sessionid as session_id
        ,date as booking_date
        ,cancelled
        ,attendance
        ,classId as class_id
        ,className as class_name
        ,classDescription as class_description
        ,notify
        ,_imported_at

    from {{source('firestore','bookings')}}
)
select * 
from raw