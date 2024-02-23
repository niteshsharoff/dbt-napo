with
    subscriptions as (
        select
            id as stripe_subscription_id,
            customer as stripe_customer_id,
            json_extract_scalar(metadata, '$.customerUuid') as customer_uuid,
            status,
            json_extract_scalar(plan, '$.interval') as payment_plan_type,
            json_extract_scalar(plan, '$.amount') as payment_amount_mu,
            json_extract_scalar(plan, '$.currency') as currency,
            json_extract_scalar(cancellation_details, '$.reason') as cancellation_reason
            ,
            timestamp_seconds(created) as created_at,
            timestamp_seconds(updated) as updated_at,
            timestamp_seconds(cast(trial_start as int64)) as trial_started_at,
            timestamp_seconds(cast(trial_end as int64)) as trial_ended_at,
            timestamp_seconds(cast(start_date as int64)) as started_at,
            timestamp_seconds(cast(ended_at as int64)) as ended_at,
            timestamp_seconds(cast(canceled_at as int64)) as cancelled_at
        from {{ source("src_airbyte", "stripe_subscriptions") }}
    ),
    final as (
        select *, max(updated_at) over (partition by stripe_subscription_id) as _latest
        from subscriptions
    )
select * except (_latest)
from final
where updated_at = _latest
