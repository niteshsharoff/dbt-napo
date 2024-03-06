{{ config(materialized="view", pre_hook=["{{claim_bdx_udfs()}}"]) }}

with
    policy_number as (
        select distinct policy_id, reference_number, start_date
        from {{ ref("stg_raw__policy") }}
    ),
    pet_age as (select distinct pet_id, date_of_birth from {{ ref("stg_raw__pet") }}),
    claim_history as (
        select
            claim_uuid,
            policy_id,
            claim_reference,
            master_claim_reference,
            status,
            date(submitted_at) as date_received,
            onset_date,
            first_invoice_date,
            last_invoice_date,
            closed_date,
            is_continuation,
            cover_type,
            sub_type as claim_sub_type,
            condition,
            decline_reason,
            invoice_amount_mu,
            paid_amount_mu,
            recovery_amount_mu,
            decision,
            product_id,
            pet_id,
            effective_from
        from {{ ref("int_claim_history") }}
    ),
    enrichment_data as (
        select
            -- claim_uuid,
            policy.reference_number as policy_number,
            claim_reference,
            master_claim_reference,
            status,
            date_received,
            onset_date,
            first_invoice_date,
            last_invoice_date,
            closed_date,
            cast(is_continuation as bool) as is_continuation,
            replace(cover_type, '_', ' ') as cover_type,
            initcap(replace(claim_sub_type, '_', ' ')) as claim_sub_type,
            condition,
            decision,
            decline_reason,
            cast(coalesce(invoice_amount_mu / 100, 0.0) as numeric) as invoice_amount,
            cast(coalesce(paid_amount_mu / 100, 0.0) as numeric) as paid_amount,
            cast(coalesce(recovery_amount_mu / 100, 0.0) as numeric) as recovery_amount,
            cast(product.excess as numeric) as product_excess,
            product.co_pay as product_copay,
            {{ target.schema }}.calculate_pet_age_months(
                pet.date_of_birth, policy.start_date
            ) as pet_age_months,
            effective_from,
            lead(effective_from, 1, timestamp("2999-01-01 00:00:00+00")) over (
                partition by claim_uuid order by effective_from
            ) as effective_to
        from claim_history c
        left join policy_number policy on c.policy_id = policy.policy_id
        left join raw.product product on c.product_id = product.id
        left join pet_age pet on c.pet_id = pet.pet_id
    ),
    bdx_fields as (
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
            {{ target.schema }}.calculate_reserve_amount(
                status,
                decision,
                invoice_amount,
                paid_amount,
                product_excess,
                pet_age_months,
                is_continuation,
                product_copay
            ) as reserve,
            {{ target.schema }}.calculate_incurred_amount(
                {{ target.schema }}.calculate_reserve_amount(
                    status,
                    decision,
                    invoice_amount,
                    paid_amount,
                    product_excess,
                    pet_age_months,
                    is_continuation,
                    product_copay
                ),
                paid_amount,
                0.0,  -- fee invoice
                recovery_amount
            ) as incurred_amount,
            recovery_amount,
            effective_from,
            effective_to
        from enrichment_data
        -- TBD: drop any transaction where the claim_reference is null
        where claim_reference is not null
    ),
    has_row_changed as (
        select
            *,
            case
                when
                    coalesce(policy_number, '') != lag(policy_number) over (
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
                    status != lag(status) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    date_received != lag(date_received) over (
                        partition by claim_reference order by effective_from
                    )
                then 1
                when
                    coalesce(onset_date, '') != coalesce(
                        lag(onset_date) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(first_invoice_date, '') != coalesce(
                        lag(first_invoice_date) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(last_invoice_date, '') != coalesce(
                        lag(last_invoice_date) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(closed_date, '') != coalesce(
                        lag(closed_date) over (
                            partition by claim_reference order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(is_continuation, false) != coalesce(
                        lag(is_continuation) over (
                            partition by claim_reference order by effective_from
                        ),
                        false
                    )
                then 1
                when
                    cover_type != lag(cover_type) over (
                        partition by claim_reference order by effective_from
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
                    coalesce(invoice_amount, 0) != coalesce(
                        lag(invoice_amount) over (
                            partition by claim_reference order by effective_from
                        ),
                        0
                    )
                then 1
                when
                    coalesce(paid_amount, 0) != coalesce(
                        lag(paid_amount) over (
                            partition by claim_reference order by effective_from
                        ),
                        0
                    )
                then 1
                when
                    coalesce(reserve, 0) != coalesce(
                        lag(reserve) over (
                            partition by claim_reference order by effective_from
                        ),
                        0
                    )
                then 1
                when
                    coalesce(incurred_amount, 0) != coalesce(
                        lag(incurred_amount) over (
                            partition by claim_reference order by effective_from
                        ),
                        0
                    )
                then 1
                when
                    coalesce(recovery_amount, 0) != coalesce(
                        lag(recovery_amount) over (
                            partition by claim_reference order by effective_from
                        ),
                        0
                    )
                then 1
                else 0
            end as row_changed
        from bdx_fields
    ),
    assign_grp_id as (
        select
            *,
            coalesce(
                sum(row_changed) over (
                    partition by claim_reference order by effective_from
                ),
                0
            ) as grp_id
        from has_row_changed
    ),
    collate_duplicates_in_grp as (
        select
            policy_number,
            claim_reference,
            master_claim_reference,
            status,
            cast(date_received as date) as date_received,
            cast(onset_date as date) as onset_date,
            cast(first_invoice_date as date) as first_invoice_date,
            cast(last_invoice_date as date) as last_invoice_date,
            cast(closed_date as date) as closed_date,
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
            min(effective_from) as effective_from
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
    )
select
    *,
    lead(effective_from, 1, timestamp("2999-01-01 00:00:00+00")) over (
        partition by claim_reference order by effective_from
    ) as effective_to
from collate_duplicates_in_grp
