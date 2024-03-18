{{ config(materialized="table") }}

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
            -- excluded when capturing row changes, used only for reporting
            multipet_number,
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
    has_row_changed as (
        select
            *,
            -- coalesce nullable fields
            case
                when
                    pet_id
                    != lag(pet_id) over (partition by pet_uuid order by effective_from)
                then 1
                when
                    pet_uuid != lag(pet_uuid) over (
                        partition by pet_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(name, '') != coalesce(
                        lag(name) over (partition by pet_uuid order by effective_from),
                        ''
                    )
                then 1
                when
                    coalesce(cast(date_of_birth as string), '') != coalesce(
                        lag(cast(date_of_birth as string)) over (
                            partition by pet_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    gender
                    != lag(gender) over (partition by pet_uuid order by effective_from)
                then 1
                when
                    coalesce(size, '') != coalesce(
                        lag(size) over (partition by pet_uuid order by effective_from),
                        ''
                    )
                then 1
                when
                    cost_pounds != lag(cost_pounds) over (
                        partition by pet_uuid order by effective_from
                    )
                then 1
                when
                    is_neutered != lag(is_neutered) over (
                        partition by pet_uuid order by effective_from
                    )
                then 1
                when
                    is_microchipped != lag(is_microchipped) over (
                        partition by pet_uuid order by effective_from
                    )
                then 1
                when
                    is_vaccinated != lag(is_vaccinated) over (
                        partition by pet_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(species, '') != coalesce(
                        lag(species) over (
                            partition by pet_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(breed_category, '') != coalesce(
                        lag(breed_category) over (
                            partition by pet_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(breed_name, '') != coalesce(
                        lag(breed_name) over (
                            partition by pet_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(breed_source, '') != coalesce(
                        lag(breed_source) over (
                            partition by pet_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    has_pre_existing_conditions
                    != lag(has_pre_existing_conditions) over (
                        partition by pet_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(change_reason, '') != coalesce(
                        lag(change_reason) over (
                            partition by pet_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                else 0
            end as row_changed
        from pet_scd
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            coalesce(
                sum(row_changed) over (partition by pet_uuid order by effective_from), 0
            ) as grp_id
        from has_row_changed
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
            is_vaccinated,
            species,
            breed_category,
            breed_name,
            breed_source,
            has_pre_existing_conditions,
            change_reason,
            multipet_number,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to
        from assign_grp_id
        group by
            pet_id,
            pet_uuid,
            name,
            date_of_birth,
            gender,
            size,
            cost_pounds,
            is_neutered,
            is_microchipped,
            is_vaccinated,
            species,
            breed_category,
            breed_name,
            breed_source,
            has_pre_existing_conditions,
            change_reason,
            multipet_number,
            grp_id
        order by pet_uuid, effective_from
    )
select *
from final
