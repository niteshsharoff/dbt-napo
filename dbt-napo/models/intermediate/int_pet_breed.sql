with pet_breeds as (
      select p.pk
            ,p.breed_category
            ,p.multipet_number
            ,p.is_neutered
            ,p.age_months
            ,p.size
            ,p.name
            ,p.species
            --,b.species as breed_species
            ,b.source as source
            ,b.name as breed_name 
            ,b.common_breed_name
      from {{ref('raw_pet')}} p
      left join {{ref('int_breed_breed_mapping')}} b
      on p.breed = b.pk
)
select distinct * from pet_breeds
