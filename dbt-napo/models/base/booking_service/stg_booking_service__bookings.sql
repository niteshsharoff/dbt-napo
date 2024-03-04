select
    uuid as booking_uuid,
    status,
    class_uuid,
    class_name,
    session_uuid,
    session_date,
    customer_uuid,
    attended_duration,
    cancellation_reason,
    free_class,
    created_at,
    updated_at
from `ae32-vpcservice-prod.booking.booking`
