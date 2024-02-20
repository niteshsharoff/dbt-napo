select
    id as customer_id,
    uuid as customer_uuid,
    payment_provider_id as stripe_customer_id,
    * except (id, uuid, payment_provider_id, created_at, updated_at, run_date),
    timestamp_millis(created_at) as created_at,
    timestamp_millis(created_at) as updated_at,
    run_date
from {{ source("raw", "booking_service_customer") }}
-- PA customers will have the source column set to 'pa_registrant' in booking-service
where source = 'pa_registrant'
