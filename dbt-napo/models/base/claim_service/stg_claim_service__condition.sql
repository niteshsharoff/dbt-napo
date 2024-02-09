{{ config(materialized="table") }}

with
    extract_cdc_events as (
        select distinct
            json_extract_scalar(payload, '$.claim_id') as claim_uuid,
            json_extract_scalar(payload, '$.condition') as condition,
            json_extract_scalar(payload, '$.onset_date') as onset_date,
            json_extract_scalar(payload, '$.is_continuation') as is_continuation,
            cast(
                json_extract_scalar(payload, '$.updated_at') as timestamp
            ) as updated_at
        from `ae32-vpcservice-prod.claim_service.public_condition`
    )
select
    *,
    updated_at as effective_from,
    lead(updated_at, 1, timestamp("2999-01-01 00:00:00+00")) over (
        partition by claim_uuid order by updated_at
    ) as effective_to
from extract_cdc_events
