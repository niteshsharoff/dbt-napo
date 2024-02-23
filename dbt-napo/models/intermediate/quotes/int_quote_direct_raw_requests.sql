with
    common_breed_map as (
        select *
        from {{ source("raw", "commonbreedmapping") }}
        where run_date = (select max(run_date) from raw.commonbreedmapping)
    ),
    direct_quotes as (
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            json_extract_scalar(
                raw_request, '$.applicant.email_address'
            ) as applicant_email_address,
            json_extract_scalar(
                raw_request, '$.applicant.address_locality'
            ) as applicant_address_locality,
            json_extract_scalar(
                raw_request, '$.applicant.address_region'
            ) as applicant_address_region,
            json_extract_scalar(
                raw_request, '$.applicant.street_address'
            ) as applicant_street_address,
            json_extract_scalar(
                raw_request, '$.applicant.postal_code'
            ) as applicant_postal_code,
            json_extract_scalar(pet, '$.name') as pet_name,
            json_extract_scalar(pet, '$.species') as pet_species,
            json_extract_scalar(pet, '$.breed_category') as pet_breed_category,
            json_extract_scalar(pet, '$.breed_name') as pet_breed_name,
            json_extract_scalar(pet, '$.gender') as pet_gender,
            json_extract_scalar(pet, '$.date_of_birth') as pet_date_of_birth,
            json_extract_scalar(raw_request, '$.start_date') as policy_start_date,
            json_extract_scalar(pet, '$.is_micro_chipped') as pet_is_microchipped,
            json_extract_scalar(pet, '$.is_aggressive') as pet_is_aggressive,
            json_extract_scalar(pet, '$.is_neutered') as pet_is_neutered,
            json_extract_scalar(pet, '$.is_vaccinated') as pet_is_vaccinated
        from {{ ref("stg_raw__quote_request", v=2) }}
        cross join unnest(json_extract_array(raw_request, '$.pets')) as pet
        where quote_source = 'direct' or quote_source = 'renewal'
    ),
    processed_quotes as (
        select
            q.quote_uuid,
            q.quote_at,
            q.status,
            q.quote_source,
            cast(null as string) as applicant_firstname,
            cast(null as string) as applicant_surname,
            cast(null as date) as applicant_dob,
            q.applicant_email_address as applicant_email,
            q.applicant_address_locality as applicant_address_1,
            q.applicant_address_region as applicant_address_2,
            q.applicant_street_address as applicant_address_3,
            cast(null as string) as applicant_address_4,
            cast(null as string) as applicant_address_5,
            q.applicant_postal_code as applicant_postcode,
            initcap(q.pet_name) as pet_name,
            safe_cast(q.pet_date_of_birth as date) as pet_dob,
            case
                when q.pet_gender = '1'
                then 'male'
                when q.pet_gender = '2'
                then 'female'
                when q.pet_gender = '9'
                then 'not_applicable'
                else 'unknown'
            end as pet_gender,
            q.pet_species,
            q.pet_breed_category,
            m.source_breed_name as pet_source_breed_name,
            m.common_breed_name as pet_common_breed_name,
            cast(q.pet_is_microchipped as bool) as pet_is_microchipped,
            cast(q.pet_is_aggressive as bool) as pet_is_aggressive,
            cast(q.pet_is_neutered as bool) as pet_is_neutered,
            cast(q.pet_is_vaccinated as bool) as pet_is_vaccinated
        from direct_quotes q
        left join
            common_breed_map m
            on lower(q.pet_breed_name) = lower(m.source_breed_name)
            and m.source = 'direct'
    )
select *
from processed_quotes
