{{ config(materialized="view") }}

with
    dim_claim as (
        select * except (effective_to), 'claim_service_db' as source
        from {{ ref("dim_claim", v=2) }}
        union distinct
        -- interpolate claim_bdx rows
        select * except (effective_to), 'claim_bdx' as source
        from {{ ref("dim_claim", v=1) }}
    )
select
    policy_number,
    claim_reference,
    master_claim_reference,
    status,
    date_received,
    onset_date,
    first_invoice_date,
    last_invoice_date,
    closed_date,
    is_continuation,
    cover_type,
    claim_sub_type,
    condition,
    decline_reason,
    invoice_amount,
    paid_amount,
    reserve,
    incurred_amount,
    recovery_amount,
    effective_from,
    lead(effective_from, 1, timestamp("2999-01-01 00:00:00+00")) over (
        partition by claim_reference order by effective_from
    ) as effective_to,
    source
from dim_claim
