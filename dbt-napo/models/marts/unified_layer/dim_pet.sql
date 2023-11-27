{{ config(schema="marts", materialized="table") }}

{% set today = modules.datetime.datetime.now() %}
{% set yesterday = (today - modules.datetime.timedelta(1)).date() %}

with
    pet_scd as (
        select
            pet_id,
            uuid as pet_uuid,
            pet.name,
            date_of_birth,
            case
                when pet.gender = '1'
                then 'male'
                when pet.gender = '2'
                then 'female'
                else null
            end as gender,
            size,
            cost as cost_pounds,
            is_neutered,
            is_microchipped,
            is_vaccinated,
            pet.species,
            pet.breed_category,
            breed.name as breed_name,
            breed.source as breed_source,
            has_pre_existing_conditions,
            change_reason as change_reason,
            effective_from,
            effective_to
        from {{ ref("stg_raw__pet_ledger") }} pet
        left join
            {{ source("raw", "breed") }} breed
            on pet.breed_id = breed.id
            -- use the latest snapshot of the policy breed map
            and breed.run_date = parse_date('%Y-%m-%d', '{{yesterday}}')
        order by pet_uuid, effective_from
    ),
    compute_row_hash as (
        select
            *,
            farm_fingerprint(
                concat(
                    pet_id,
                    pet_uuid,
                    name,
                    coalesce(cast(date_of_birth as string), ''),
                    gender,
                    coalesce(size, ''),
                    cost_pounds,
                    is_neutered,
                    is_microchipped,
                    is_vaccinated,
                    species,
                    breed_category,
                    coalesce(breed_name, ''),
                    coalesce(breed_source, ''),
                    coalesce(change_reason, ''),
                    breed_source,
                    has_pre_existing_conditions,
                    change_reason
                )
            ) as row_hash
        from pet_scd
    ),
    tag_changes as (
        select
            *,
            -- identify rows that have changed
            coalesce(
                row_hash
                <> lag(row_hash) over (partition by pet_uuid order by effective_from),
                true
            ) as has_changed
        from compute_row_hash
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            sum(cast(has_changed as integer)) over (
                partition by pet_uuid order by effective_from
            ) as grp_id
        from tag_changes
    ),
    final as (
        select
            pet_id,
            pet_uuid,
            name,
            date_of_birth,
            gender,
            size,
            cost_pounds,
            is_neutered,
            is_microchipped,
            is_vaccinated,  -- 10
            species,
            breed_category,
            breed_name,
            breed_source,
            has_pre_existing_conditions,
            change_reason,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to
        from assign_grp_id
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, grp_id
        order by pet_uuid, effective_from
    )
select *
from final
