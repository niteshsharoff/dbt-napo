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
