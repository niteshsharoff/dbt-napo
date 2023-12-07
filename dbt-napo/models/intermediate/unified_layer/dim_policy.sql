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
    has_row_changed as (
        select
            *,
            case
                when
                    policy_id != lag(policy_id) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    quote_uuid != lag(quote_uuid) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    customer_uuid != lag(customer_uuid) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    pet_uuid != lag(pet_uuid) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    policy_uuid != lag(policy_uuid) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    reference_number != lag(reference_number) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(quote_source_reference, '') != coalesce(
                        lag(quote_source_reference) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    payment_plan_type != lag(payment_plan_type) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    retail_price != lag(retail_price) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(notes, '') != coalesce(
                        lag(notes) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    accident_cover_start_date != lag(accident_cover_start_date) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    start_date != lag(start_date) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    end_date != lag(end_date) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(cast(cancel_date as string), '') != coalesce(
                        lag(cast(cancel_date as string)) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cancel_detail, '') != coalesce(
                        lag(cancel_detail) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cancel_reason, '') != coalesce(
                        lag(cancel_reason) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    created_date != lag(created_date) over (
                        partition by policy_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(cast(sold_at as string), '') != coalesce(
                        lag(cast(sold_at as string)) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(cancelled_at as string), '') != coalesce(
                        lag(cast(cancelled_at as string)) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(cast(reinstated_at as string), '') != coalesce(
                        lag(cast(reinstated_at as string)) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(change_reason, '') != coalesce(
                        lag(change_reason) over (
                            partition by policy_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                else 0
            end as row_changed
        from policy_scd
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            coalesce(
                sum(row_changed) over (
                    partition by policy_uuid order by effective_from
                ),
                0
            ) as grp_id
        from has_row_changed
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
            notes,
            accident_cover_start_date,
            start_date,
            end_date,
            cancel_date,
            cancel_detail,
            cancel_reason,
            created_date,
            sold_at,
            cancelled_at,
            reinstated_at,
            change_reason,
            product_id,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to
        from assign_grp_id
        group by
            policy_id,
            policy_uuid,
            quote_uuid,
            customer_uuid,
            pet_uuid,
            reference_number,
            quote_source_reference,
            payment_plan_type,
            retail_price,
            notes,
            accident_cover_start_date,
            start_date,
            end_date,
            cancel_date,
            cancel_detail,
            cancel_reason,
            created_date,
            sold_at,
            cancelled_at,
            reinstated_at,
            change_reason,
            product_id,
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
