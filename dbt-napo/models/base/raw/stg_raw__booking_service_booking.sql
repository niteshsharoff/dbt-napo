select
    id as booking_id,
    uuid as booking_uuid,
    * except (
        id,
        uuid,
        session_date,
        created_at,
        updated_at,
        sms_reminder_id,
        email_reminder_id,
        run_date
    ),
    timestamp_millis(session_date) as session_date,
    timestamp_millis(created_at) as created_at,
    timestamp_millis(updated_at) as updated_at,
    run_date
from {{ source("raw", "booking_service_booking") }}
