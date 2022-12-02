with raw as (
      select *
            ,time(booking_date) as booking_time
            ,EXTRACT(DAYOFWEEK from booking_date) as booking_weekday
      from {{ref('stg_firestore__bookings')}}
)
select a.* except (booking_weekday)
      ,b.weekday as booking_weekday
from raw a
left join {{ref('lookup_weekday_mapping')}} b
on a.booking_weekday = b.no