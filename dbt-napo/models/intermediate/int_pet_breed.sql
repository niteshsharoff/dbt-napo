with
    pet_breeds as (
        select
            p.pk,
            p._petid as pet_id,
            p.breed_category,
            p.multipet_number,
            p.is_neutered,
            p.age_months,
            p.date_of_birth as pet_date_of_birth,
            p.size,
            p.name,
            p.species,
            -- ,b.species as breed_species
            b.source as source,
            b.name as breed_name,
            b.common_breed_name
        from {{ ref("stg_postgres__pet") }} p
        left join {{ ref("int_breed_breed_mapping") }} b on p.breed = b.pk
    )
select distinct *
from pet_breeds
