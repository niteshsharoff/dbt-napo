with
    common_breed_map as (
        select *
        from {{ source("raw", "commonbreedmapping") }}
        where
            run_date
            = (select max(run_date) from {{ source("raw", "commonbreedmapping") }})
    ),
    denormalised_quotes as (
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            json_extract_scalar(
                raw_request, '$.customer.first_name'
            ) as applicant_firstname,
            json_extract_scalar(
                raw_request, '$.customer.last_name'
            ) as applicant_surname,
            json_extract_scalar(
                raw_request, '$.customer.date_of_birth'
            ) as applicant_dob,
            json_extract_scalar(
                raw_request, '$.customer.email_address'
            ) as applicant_email,
            json_extract_scalar(
                raw_request, '$.customer.address_locality'
            ) as applicant_address_locality,
            json_extract_scalar(
                raw_request, '$.customer.address_region'
            ) as applicant_address_region,
            json_extract_scalar(
                raw_request, '$.customer.street_address'
            ) as applicant_street_address,
            json_extract_scalar(
                raw_request, '$.customer.postal_code'
            ) as applicant_postcode,
            json_extract_scalar(pet, '$.name') as pet_name,
            json_extract_scalar(pet, '$.species') as pet_species,
            json_extract_scalar(pet, '$.date_of_birth') as pet_dob,
            json_extract_scalar(pet, '$.gender') as pet_gender,
            json_extract_scalar(pet, '$.cost') as pet_cost,
            json_extract_scalar(pet, '$.source_breed_name') as pet_breed_name,
            json_extract_scalar(pet, '$.breed_category') as pet_breed_category,
            json_extract_scalar(pet, '$.is_aggressive') as pet_is_aggressive,
            json_extract_scalar(pet, '$.is_microchipped') as pet_is_microchipped,
            json_extract_scalar(pet, '$.is_neutered') as pet_is_neutered,
            json_extract_scalar(pet, '$.is_vaccinated') as pet_is_vaccinated
        from {{ ref("stg_raw__quote_request", v=2) }}
        cross join unnest(json_extract_array(raw_request, '$.pets')) as pet
        where quote_source = 'quotezone'
    ),
    processed_quotes as (
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            initcap(applicant_firstname) as applicant_firstname,
            initcap(applicant_surname) as applicant_surname,
            cast(applicant_dob as date) as applicant_dob,
            applicant_email,
            applicant_address_locality,
            applicant_address_region,
            applicant_street_address,
            applicant_postcode,
            initcap(pet_name) as pet_name,
            pet_species,
            safe_cast(pet_dob as date) as pet_dob,
            case
                when pet_gender = '1'
                then 'male'
                when pet_gender = '2'
                then 'female'
                else 'unknown'
            end as pet_gender,
            cast(pet_cost as numeric) as pet_cost,
            pet_breed_name,
            pet_breed_category,
            cast(pet_is_aggressive as bool) as pet_is_aggressive,
            cast(pet_is_microchipped as bool) as pet_is_microchipped,
            cast(pet_is_neutered as bool) as pet_is_neutered,
            cast(pet_is_vaccinated as bool) as pet_is_vaccinated
        from denormalised_quotes
    ),
    final as (
        select
            q.quote_uuid,
            q.quote_at,
            q.status,
            q.quote_source,
            q.applicant_firstname,
            q.applicant_surname,
            q.applicant_dob,
            q.applicant_email,
            q.applicant_street_address as applicant_address_1,
            q.applicant_address_locality as applicant_address_2,
            q.applicant_address_region as applicant_address_3,
            cast(null as string) as applicant_address_4,
            cast(null as string) as applicant_address_5,
            q.applicant_postcode,
            q.pet_name,
            q.pet_dob,
            q.pet_gender,
            q.pet_species,
            q.pet_breed_category,
            m.source_breed_name as pet_source_breed_name,
            m.common_breed_name as pet_common_breed_name,
            q.pet_is_microchipped,
            q.pet_is_aggressive,
            q.pet_is_neutered,
            q.pet_is_vaccinated
        from processed_quotes q
        left join
            common_breed_map m
            on q.pet_breed_name = m.source_breed_name
            and q.quote_source = m.source
    )
select *
from final
