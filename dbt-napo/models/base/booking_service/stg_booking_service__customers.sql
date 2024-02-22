select
    id as customer_id,
    uuid as customer_uuid,
    payment_provider_id as stripe_customer_id,
    created_at,
    updated_at
from `ae32-vpcservice-prod.booking.customer`
-- PA customers will have the source column set to 'pa_registrant' in booking-service
where source = 'pa_registrant'
