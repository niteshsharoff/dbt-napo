select
    id as customer_id,
    uuid as customer_uuid,
    * except (id, uuid, created_at, firebase_uid, run_date),
    timestamp_millis(created_at) as created_at,
    run_date
from {{ source("raw", "booking_service_customer") }}
