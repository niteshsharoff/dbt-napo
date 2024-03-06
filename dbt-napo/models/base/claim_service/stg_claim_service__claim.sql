{{ config(materialized="table") }}

with
    extract_cdc_events as (
        select distinct
            json_extract_scalar(payload, '$.claim_id') as claim_uuid,
            json_extract_scalar(payload, '$.claim_reference') as claim_reference,
            json_extract_scalar(
                payload, '$.master_claim_reference'
            ) as master_claim_reference,
            json_extract_scalar(payload, '$.status') as status,
            json_extract_scalar(payload, '$.sub_type') as sub_type,
            json_extract_scalar(payload, '$.cover_type') as cover_type,
            json_extract_scalar(payload, '$.submitted_at') as submitted_at,
            json_extract_scalar(payload, '$.first_invoice_date') as first_invoice_date,
            json_extract_scalar(payload, '$.last_invoice_date') as last_invoice_date,
            json_extract_scalar(payload, '$.closed_date') as closed_date,
            cast(
                json_extract_scalar(payload, '$.recovery_cents') as int
            ) as recovery_amount_mu,
            json_extract_scalar(payload, '$.decision') as decision,
            json_extract_scalar(payload, '$.decline_reason') as decline_reason,
            -- link to policy dimension
            cast(json_extract_scalar(payload, '$.policy_id') as int) as policy_id,
            cast(json_extract_scalar(payload, '$.product_id') as int) as product_id,
            cast(json_extract_scalar(payload, '$.pet_id') as int) as pet_id,
            cast(json_extract_scalar(payload, '$.customer_id') as int) as customer_id,
            cast(
                json_extract_scalar(payload, '$.updated_at') as timestamp
            ) as updated_at
        from `ae32-vpcservice-prod.claim_service.public_claim`
    )
select
    *,
    updated_at as effective_from,
    lead(updated_at, 1, timestamp("2999-01-01 00:00:00+00")) over (
        partition by claim_uuid order by updated_at
    ) as effective_to
from extract_cdc_events
