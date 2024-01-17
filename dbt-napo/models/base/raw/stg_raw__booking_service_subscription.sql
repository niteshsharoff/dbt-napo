select
    id as subscription_id,
    uuid as subscription_uuid,
    status,
    customer_uuid,
    payment_provider_id,
    timestamp_millis(created_at) as created_at,
    timestamp_millis(updated_at) as updated_at,
    run_date
from {{ source("raw", "booking_service_subscription") }}
