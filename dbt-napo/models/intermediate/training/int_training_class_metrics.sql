with
    class_data as (
        select distinct
            customer_uuid,
            class_uuid,
            class_name,
            status,
            attended_duration,
            session_date,
            created_at,
            min(created_at) over (
                partition by customer_uuid, class_uuid, status order by created_at
            ) as _first_instance_at,
            first_value(
                case when status = 'ATTENDED' then class_name else null end
            ) over (
                partition by customer_uuid
                order by case when status = 'ATTENDED' then 0 else 1 end, session_date
            ) as first_class_attended
        from {{ ref("stg_booking_service__bookings") }}
        order by customer_uuid
    ),
    -- A class can have multiple sessions, we only count the status of the first
    -- session for a giving class
    filter_earliest_status_occurence as (
        select customer_uuid, class_uuid, class_name, status, first_class_attended
        from class_data
        where created_at = _first_instance_at
    ),
    pivot_metric_to_class_grain as (
        select *
        from
            filter_earliest_status_occurence pivot (
                sum(1) for status in (
                    'SCHEDULED' as scheduled,
                    'JOINED' as joined,
                    'ATTENDED' as attended,
                    'MISSED' as missed,
                    'CANCELLED' as cancelled
                )
            )
    ),
    backfill_class_metrics as (
        select
            customer_uuid,
            class_uuid,
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
            first_class_attended
        from pivot_metric_to_class_grain
    )
select *
from backfill_class_metrics
