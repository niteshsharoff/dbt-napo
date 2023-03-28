with a as (
    select 
        id
       ,firstName as first_name
       ,lastName as last_name
       ,email
       ,phone
       ,courses
       ,_imported_at
    from {{source('firestore','users')}}
)
select * 
from a