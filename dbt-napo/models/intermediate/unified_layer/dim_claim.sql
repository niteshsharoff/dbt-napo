{{ config(materialized="table") }}

with
    bdx_claim_history as (
        select
            policy_number as policy_number,
            coalesce(claimid, master_claim_id) as claim_reference,
            master_claim_id as master_claim_reference,
            claim_substatus as status,
            claim_date as date_received,
            incident_date as onset_date,
            treatment_from as first_invoice_date,
            treatment_to as last_invoice_date,
            paid_period as closed_date,
            continuation_claim as is_continuation,
            cover_type as cover_type,
            claim_type as claim_sub_type,
            reason_for_claim as condition,
            decline_reason as decline_reason,
            coalesce(total_value_of_claim, 0.0) as invoice_amount,
            coalesce(claims_paid_to_date, 0.0) as paid_amount,
            coalesce(reserve, 0.0) as reserve,
            coalesce(incurred_value, 0.0) as incurred_amount,
            coalesce(recovery, 0.0) as recovery_amount,
            bdx_nominal_date as effective_from,
            lead(bdx_nominal_date, 1, '2999-01-01') over (
                partition by claimid order by bdx_nominal_date
            ) as effective_to
        from {{ ref("stg_raw__claim_bdx") }}
    ),
    compute_row_hash as (
        select
            *,
            farm_fingerprint(
                concat(
                    policy_number,
                    coalesce(claim_reference, ''),
                    coalesce(master_claim_reference, ''),
                    coalesce(status, ''),
                    coalesce(cast(date_received as string), ''),
                    coalesce(cast(onset_date as string), ''),
                    coalesce(cast(first_invoice_date as string), ''),
                    coalesce(cast(last_invoice_date as string), ''),
                    coalesce(cast(closed_date as string), ''),
                    coalesce(cast(is_continuation as string), ''),
                    coalesce(cover_type, ''),
                    coalesce(claim_sub_type, ''),
                    coalesce(condition, ''),
                    coalesce(decline_reason, ''),
                    invoice_amount,
                    paid_amount,
                    reserve,
                    incurred_amount,
                    recovery_amount
                )
            ) as row_hash
        from bdx_claim_history
    ),
    tag_changes as (
        select
            *,
            -- identify rows that have changed
            coalesce(
                row_hash <> lag(row_hash) over (
                    partition by claim_reference order by effective_from
                ),
                true
            ) as has_changed
        from compute_row_hash
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            sum(cast(has_changed as integer)) over (
                partition by claim_reference order by effective_from
            ) as grp_id
        from tag_changes
    ),
    final as (
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
            is_continuation,  -- 10
            cover_type,
            claim_sub_type,
            condition,
            decline_reason,
            invoice_amount,
            paid_amount,
            reserve,
            incurred_amount,
            recovery_amount,
            min(effective_from) as effective_from,  -- 20
            max(effective_to) as effective_to
        from assign_grp_id
        group by
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, grp_id
        order by claim_reference, effective_from
    )
select *
from final
order by claim_reference, master_claim_reference, effective_from
