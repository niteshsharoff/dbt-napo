select
    payment_provider_id as stripe_customer_id,
    uuid as customer_uuid,
    email,
    timestamp_millis(created_at) as created_at,
    timestamp_millis(created_at) as updated_at
from {{ source("raw", "booking_service_customer") }}
-- PA customers will have the source column set to 'pa_registrant' in booking-service
where source = 'pa_registrant'
