{{ config(materialized="table") }}

with
    extract_cdc_events as (
        select distinct
            json_extract_scalar(payload, '$.claim_id') as claim_uuid,
            cast(
                json_extract_scalar(payload, '$.invoice_amount_in_pence') as int
            ) as invoice_amount_mu,
            cast(
                json_extract_scalar(payload, '$.paid_amount_in_pence') as int
            ) as paid_amount_mu,
            cast(
                json_extract_scalar(payload, '$.excess_in_pence') as int
            ) as excess_amount_mu,
            json_extract_scalar(payload, '$.currency') as currency,
            cast(
                json_extract_scalar(payload, '$.updated_at') as timestamp
            ) as updated_at
        from `ae32-vpcservice-prod.claim_service.public_payment_breakdown`
    )
select
    *,
    updated_at as effective_from,
    lead(updated_at, 1, timestamp("2999-01-01 00:00:00+00")) over (
        partition by claim_uuid order by updated_at
    ) as effective_to
from extract_cdc_events
