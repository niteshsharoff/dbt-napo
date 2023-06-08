 select * except(id,uuid,created_at,firebase_uid)
 ,id as customer_id
 ,uuid as customer_uuid
 ,timestamp_millis(created_at) as created_at
from {{ source('raw', 'booking_service_customer') }}
