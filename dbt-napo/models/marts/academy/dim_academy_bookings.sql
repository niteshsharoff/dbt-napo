

with raw as (
select 
     a.id as id
    ,a.uuid
    ,a.status
    ,a.class_uuid as class_uuid
    ,a.class_name
    ,a.session_uuid
    ,a.session_date as class_date
    ,time(a.session_date) as class_time
    ,EXTRACT(DAYOFWEEK from a.session_date) as class_weekday
    ,a.customer_uuid
    ,b.customer_id
    ,a.created_at
    ,a.updated_at
    ,a.soft_delete
    ,a.run_date
from {{ref('stg_raw__booking_service_booking')}} a
left join {{ref('stg_raw__booking_service_customer')}} b
using(customer_uuid)
)
select a.* except(class_weekday)
      ,b.weekday as class_weekday
from raw a
left join {{ref('lookup_weekday_mapping')}} b
on a.class_weekday = b.no


--Original dim academy bookings
/*
with raw as (
      select *
            ,time(class_date) as class_time
            ,EXTRACT(DAYOFWEEK from class_date) as class_weekday
      from {{ref('stg_firestore__bookings')}}
)
select a.* except (class_weekday)
      ,b.weekday as class_weekday
from raw a
left join {{ref('lookup_weekday_mapping')}} b
on a.class_weekday = b.no
*/

--This is a parity attempt with the diff data sources

/*
with raw as (
select 
     a.id as id
    ,a.uuid
    ,a.status
    ,a.class_uuid as class_id
    ,a.class_name
    ,a.sms_reminder_id
    ,a.session_uuid
    ,a.session_date as class_date
    ,time(a.session_date) as class_time
    ,EXTRACT(DAYOFWEEK from a.session_date) as class_weekday
    ,a.customer_uuid
    ,b.customer_id as user_id
    ,a.email_reminder_id
    ,a.created_at
    ,a.updated_at
    ,a.soft_delete
    ,a.run_date
from {{ref('stg_raw__booking_service_booking')}} a
left join {{ref('stg_raw__booking_service_customer')}} b
using(customer_uuid)
)
select a.* except(class_weekday)
      ,b.weekday as class_weekday
from raw a
left join {{ref('lookup_weekday_mapping')}} b
on a.class_weekday = b.no
*/

