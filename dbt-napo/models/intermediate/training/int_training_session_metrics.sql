with
    session_data as (
        select distinct
            customer_uuid,
            class_uuid,
            class_name,
            session_uuid,
            status,
            attended_duration,
            session_date,
            created_at
        from {{ ref("stg_booking_service__bookings") }}
        order by customer_uuid, class_uuid
    ),
    pivot_metric_to_session_grain as (
        select *
        from
            session_data pivot (
                sum(1) for status in (
                    'SCHEDULED' as scheduled,
                    'JOINED' as joined,
                    'ATTENDED' as attended,
                    'MISSED' as missed,
                    'CANCELLED' as cancelled
                )
            )
    ),
    backfill_session_metrics as (
        select
            customer_uuid,
            session_uuid,
            class_name,
            case
                when joined is not null
                then 1
                when attended is not null
                then 1
                when missed is not null
                then 1
                when cancelled is not null
                then 1
                else scheduled
            end as scheduled,
            case when attended is not null then 1 else joined end as joined,
            attended,
            missed,
            cancelled,
            attended_duration
        from pivot_metric_to_session_grain
    )
select *
from backfill_session_metrics
