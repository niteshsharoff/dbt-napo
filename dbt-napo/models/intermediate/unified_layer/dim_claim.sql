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
    has_row_changed as (
        select
            *,
            case
                when
                    policy_number != lag(policy_number) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    coalesce(claim_reference, '') != coalesce(
                        lag(claim_reference) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(master_claim_reference, '') != coalesce(
                        lag(master_claim_reference) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(status, '') != coalesce(
                        lag(status) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(date_received as string), '') != coalesce(
                        lag(cast(date_received as string)) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(onset_date as string), '') != coalesce(
                        lag(cast(onset_date as string)) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(first_invoice_date as string), '') != coalesce(
                        lag(cast(first_invoice_date as string)) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(last_invoice_date as string), '') != coalesce(
                        lag(cast(last_invoice_date as string)) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(closed_date as string), '') != coalesce(
                        lag(cast(closed_date as string)) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(is_continuation as string), '') != coalesce(
                        lag(cast(is_continuation as string)) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cover_type, '') != coalesce(
                        lag(cover_type) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(claim_sub_type, '') != coalesce(
                        lag(claim_sub_type) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(condition, '') != coalesce(
                        lag(condition) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(decline_reason, '') != coalesce(
                        lag(decline_reason) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    invoice_amount != lag(invoice_amount) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    paid_amount != lag(paid_amount) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    reserve != lag(reserve) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    incurred_amount != lag(incurred_amount) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    recovery_amount != lag(recovery_amount) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                else 0
            end as row_changed
        from bdx_claim_history
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            coalesce(
                sum(row_changed) over (
                    partition by claim_reference order by effective_from
                ),
                0
            ) as grp_id
        from has_row_changed
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
            min(effective_from) as effective_from,
            max(effective_to) as effective_to
        from assign_grp_id
        group by
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
            grp_id
        order by claim_reference, effective_from
    )
select *
from final
order by claim_reference, master_claim_reference, effective_from
