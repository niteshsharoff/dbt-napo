select
    content_uuid,
    customer_uuid,
    event_type,
    seconds_watched,
    video_length_seconds,
    occurred_at,
    created_at,
    updated_at
from `ae32-vpcservice-prod.booking.video_stats`
