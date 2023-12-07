{{
    config(
        materialized="table",
        pre_hook=["{{declare_underwriter_udfs()}}"],
    )
}}


with
    customer_uuid as (
        select distinct customer_id, uuid from {{ source("raw", "customer") }}
    ),
    pet_uuid as (select distinct pet_id, uuid from {{ source("raw", "pet") }}),
    policy_scd as (
        select
            policy_id,
            policy.quote_id as quote_uuid,
            cu.uuid as customer_uuid,
            pe.uuid as pet_uuid,
            policy.uuid as policy_uuid,
            reference_number,
            quote_source_reference,
            payment_plan_type,
            annual_price as retail_price,
            notes,
            accident_cover_start_date,
            start_date,
            end_date,
            cancel_date,
            cancel_detail,
            cancel_mapping.cancel_reason,
            created_date,
            sold_at,
            cancelled_at,
            reinstated_at,
            change_reason,
            product_id,
            effective_from,
            effective_to
        from {{ ref("stg_raw__policy_ledger") }} policy
        left join
            {{ ref("lookup_policy_cancel_reason") }} cancel_mapping
            on policy.cancel_reason = cancel_mapping.id
        left join customer_uuid cu on policy.customer_id = cu.customer_id
        left join pet_uuid pe on policy.pet_id = pe.pet_id
    ),
    compute_row_hash as (
        select
            *,
            farm_fingerprint(
                concat(
                    policy_id,
                    quote_uuid,
                    customer_uuid,
                    pet_uuid,
                    policy_uuid,
                    reference_number,
                    coalesce(quote_source_reference, ''),
                    payment_plan_type,
                    retail_price,
                    coalesce(notes, ''),
                    accident_cover_start_date,
                    start_date,
                    end_date,
                    coalesce(cast(cancel_date as string), ''),
                    coalesce(cancel_detail, ''),
                    coalesce(cancel_reason, ''),
                    created_date,
                    coalesce(cast(sold_at as string), ''),
                    coalesce(cast(cancelled_at as string), ''),
                    coalesce(cast(reinstated_at as string), ''),
                    coalesce(change_reason, '')
                )
            ) as row_hash
        from policy_scd
    ),
    group_changes as (
        select
            *,
            -- identify rows that have changed
            coalesce(
                row_hash <> lag(row_hash) over (
                    partition by policy_uuid order by effective_from
                ),
                true
            ) as has_changed
        from compute_row_hash
    ),
    assign_group_id as (
        select
            *,
            -- assign changes to buckets
            sum(cast(has_changed as integer)) over (
                partition by policy_uuid order by effective_from
            ) as grp_id
        from group_changes
    ),
    merge_duplicates as (
        select
            policy_id,
            policy_uuid,
            quote_uuid,
            customer_uuid,
            pet_uuid,
            reference_number,
            quote_source_reference,
            payment_plan_type,
            retail_price,
            notes,  -- 10
            accident_cover_start_date,
            start_date,
            end_date,
            cancel_date,
            cancel_detail,
            cancel_reason,
            created_date,
            sold_at,
            cancelled_at,
            reinstated_at,  -- 20
            change_reason,
            product_id,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to
        from assign_group_id
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            grp_id
        order by policy_uuid, effective_from
    ),
    product as (
        select
            id,
            reference as product_reference,
            name as product_name,
            vet_fee_cover,
            complementary_treatment_cover,
            dental_cover,
            emergency_boarding_cover,
            third_person_liability_excess,
            third_person_liability_cover,
            pet_death_cover,
            travel_cover,
            missing_pet_cover,
            behavioural_treatment_cover,
            co_pay,
            excess
        from {{ ref("stg_raw__product") }}
    ),
    final as (
        select
            po.* except (product_id, effective_from, effective_to),
            pr.* except (id),
            po.effective_from,
            po.effective_to
        from merge_duplicates po
        left join product pr on po.product_id = pr.id
    )
select *
from final
