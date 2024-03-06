{{ config(schema="marts") }}

with
    call_with_trainers as (
        select
            receipt_email as email,
            count(distinct stripe_charge_id) over (
                partition by receipt_email
            ) as trainer_sessions_booked
        from {{ ref("int_training_payments") }}
        where lower(description) like '%call with a trainer%'
    ),
    customer_metrics as (
        select
            customer_uuid,
            email,
            'pa_standalone' as customer_type,
            registration_date,
            cast(subscription_created_at as date) as purchase_date,
            cast(subscription_cancelled_at as date) as subscription_cancel_date,
            is_insurance_customer,
            is_training_customer
        from {{ ref("int_training_customers") }}
    ),
    agg_session_metrics as (
        select
            customer_uuid,
            sum(scheduled) as sessions_scheduled,
            sum(joined) as sessions_joined,
            sum(attended) as sessions_attended,
            sum(missed) as sessions_missed,
            sum(cancelled) as sessions_cancelled,
            sum(attended_duration) as sessions_time_spent
        from {{ ref("int_training_session_metrics") }}
        group by 1
    ),
    agg_class_metrics as (
        select
            customer_uuid,
            sum(scheduled) as classes_scheduled,
            sum(joined) as classes_joined,
            sum(attended) as classes_attended,
            sum(missed) as classes_missed,
            sum(cancelled) as classes_cancelled,
            any_value(first_class_attended) as first_class_attended
        from {{ ref("int_training_class_metrics") }}
        group by 1
    ),
    agg_video_metrics as (
        select distinct
            customer_uuid,
            sum(case when event_type = 'played' then 1 else 0 end) over (
                partition by customer_uuid
            ) as videos_played,
            sum(case when event_type = 'watched_85_percent' then 1 else 0 end) over (
                partition by customer_uuid
            ) as videos_completed
        from {{ ref("stg_booking_service__video_stats") }}
    ),
    media_metrics as (
        select
            coalesce(session.customer_uuid, video.customer_uuid) as customer_uuid,
            session.sessions_scheduled as sessions_scheduled,
            session.sessions_joined as sessions_joined,
            session.sessions_attended as sessions_attended,
            session.sessions_missed as sessions_missed,
            session.sessions_cancelled as sessions_cancelled,
            session.sessions_time_spent as sessions_time_spent,
            class.classes_scheduled as classes_scheduled,
            class.classes_joined as classes_joined,
            class.classes_attended as classes_attended,
            class.classes_missed as classes_missed,
            class.classes_cancelled as classes_cancelled,
            video.videos_played as videos_played,
            video.videos_completed as videos_completed,
            class.first_class_attended as first_class_attended
        from agg_class_metrics class
        -- The distinct number of customer_uuids in both class and session tables
        -- should be identical
        join agg_session_metrics session on class.customer_uuid = session.customer_uuid
        full outer join
            agg_video_metrics video on session.customer_uuid = video.customer_uuid
    ),
    final as (
        select
            customer.customer_uuid,
            coalesce(customer.email, calls.email) as email,
            customer_type,
            registration_date,
            purchase_date,
            subscription_cancel_date,
            coalesce(trainer_sessions_booked, 0) as talk_to_trainer_sessions_booked,
            coalesce(engagement.sessions_scheduled, 0) as sessions_scheduled,
            coalesce(engagement.sessions_joined, 0) as sessions_joined,
            coalesce(engagement.sessions_attended, 0) as sessions_attended,
            coalesce(engagement.sessions_missed, 0) as sessions_missed,
            coalesce(engagement.sessions_cancelled, 0) as sessions_cancelled,
            coalesce(engagement.sessions_time_spent, 0) as sessions_time_spent_seconds,
            coalesce(engagement.classes_scheduled, 0) as classes_scheduled,
            coalesce(engagement.classes_joined, 0) as classes_joined,
            coalesce(engagement.classes_attended, 0) as classes_attended,
            coalesce(engagement.classes_missed, 0) as classes_missed,
            coalesce(engagement.classes_cancelled, 0) as classes_cancelled,
            coalesce(engagement.videos_played, 0) as videos_played,
            coalesce(engagement.videos_completed, 0) as videos_completed,
            engagement.first_class_attended as first_class_attended,
            is_insurance_customer,
            is_training_customer
        from customer_metrics customer
        full outer join call_with_trainers calls on customer.email = calls.email
        left join
            media_metrics engagement
            on customer.customer_uuid = engagement.customer_uuid
    )
select *
from final
