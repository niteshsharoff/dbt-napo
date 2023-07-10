with
    raw as (
        select
            a.id as id,
            a.uuid,
            a.status,
            a.class_uuid as class_uuid,
            a.class_name,
            a.session_uuid,
            a.session_date as class_date,
            time(a.session_date) as class_time,
            extract(dayofweek from a.session_date) as class_weekday,
            a.customer_uuid,
            b.customer_id,
            a.created_at,
            a.updated_at,
            a.soft_delete,
            a.run_date
        from {{ ref("stg_raw__booking_service_booking") }} a
        left join {{ ref("stg_raw__booking_service_customer") }} b using (customer_uuid)
    ),
    temp as (
        select a.* except (class_weekday), b.weekday as class_weekday
        from raw a
        left join {{ ref("lookup_weekday_mapping") }} b on a.class_weekday = b.no
    ),
    final as (
        select
            class_uuid,
            class_name,
            class_date,
            class_time,
            class_weekday,
            customer_uuid,
            -- ,customer_id
            session_uuid,
            status,
            run_date,
            row_number() over (
                partition by class_uuid, class_date, customer_uuid
                order by run_date desc, updated_at desc
            ) as row_no
        from temp
        qualify row_no = 1
    )
select * except (row_no, run_date), run_date as _run_date
from final
