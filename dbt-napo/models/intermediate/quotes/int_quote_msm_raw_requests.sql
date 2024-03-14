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
                raw_request, '$.enquiry.applicant.address.buildingNumber'
            ) as applicant_building_number,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.address.thoroughfare'
            ) as applicant_thoroughfare,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.address.postTown'
            ) as applicant_posttown,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.address.county'
            ) as applicant_county,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.address.postcode'
            ) as applicant_postcode,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.dateOfBirth'
            ) as applicant_dob,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.emailAddress'
            ) as applicant_email,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.firstName'
            ) as applicant_firstname,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.lastName'
            ) as applicant_surname,
            json_extract_scalar(
                raw_request, '$.enquiry.applicant.phoneNumber'
            ) as applicant_phone_number,
            json_extract_scalar(pet, '$.name') as pet_name,
            json_extract_scalar(pet, '$.breed') as pet_breed_code,
            json_extract_scalar(pet, '$.breedCategory') as pet_breed_category,
            json_extract_scalar(pet, '$.gender') as pet_gender,
            json_extract_scalar(pet, '$.cost') as pet_cost,
            json_extract_scalar(pet, '$.dateOfBirth') as pet_dob,
            json_extract_scalar(pet, '$.size') as pet_size,
            json_extract_scalar(pet, '$.species') as pet_species,
            pet
        from {{ ref("stg_raw__quote_request", v=2) }}
        cross join unnest(json_extract_array(raw_request, '$.enquiry.pets')) as pet
        where quote_source = 'moneysupermarket'
    ),
    extract_warranty_questions as (
        select
            * except (pet, warranty_questions),
            json_extract_scalar(warranty_questions, '$.questionCode') as question_code,
            json_extract_scalar(warranty_questions, '$.answer') as answer
        from denormalised_quotes
        cross join
            unnest(json_extract_array(pet, '$.warrantyQuestions')) as warranty_questions
    ),
    pivot_questions_to_quote_grain as (
        select *
        from
            extract_warranty_questions pivot (
                max(answer) for question_code in (
                    -- https://github.com/CharlieNapo/quote-service/blob/main/quote/mapping/money_super_market.py#L349
                    '101' as is_neutered,
                    '102' as is_microchipped,
                    '103' as is_vaccinated,
                    '104' as is_aggressive
                )
            )
    ),
    processed_quotes as (
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            applicant_building_number,
            applicant_thoroughfare,
            applicant_posttown,
            applicant_county,
            applicant_postcode,
            cast(cast(applicant_dob as timestamp) as date) as applicant_dob,
            applicant_email,
            initcap(applicant_firstname) as applicant_firstname,
            initcap(applicant_surname) as applicant_surname,
            applicant_phone_number,
            initcap(pet_name) as pet_name,
            pet_breed_code,
            case
                when pet_breed_category = '100' or pet_breed_category = '103'
                then 'pedigree'
                when pet_breed_category = '101'
                then 'cross'
                when pet_breed_category = '102' or pet_breed_category = '104'
                then 'mixed'
                else 'unknown'
            end as pet_breed_category,
            case
                when pet_gender = '100'
                then 'male'
                when pet_gender = '101'
                then 'female'
                else 'unknown'
            end as pet_gender,
            cast(pet_cost as numeric) as pet_cost,
            cast(cast(pet_dob as timestamp) as date) as pet_dob,
            case
                when pet_size = '100' or pet_size = '103'
                then 'up to 10kg'
                when pet_size = '101' or pet_size = '104'
                then '10-20kg'
                when pet_size = '102' or pet_size = '105'
                then '20kg+'
                else 'unknown'
            end as pet_size,
            case
                when pet_species = '100'
                then 'dog'
                when pet_species = '101'
                then 'cat'
                else 'unknown'
            end as pet_species,
            cast(is_microchipped as bool) as pet_is_microchipped,
            cast(is_aggressive as bool) as pet_is_aggressive,
            cast(is_neutered as bool) as pet_is_neutered,
            cast(is_vaccinated as bool) as pet_is_vaccinated
        from pivot_questions_to_quote_grain q
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
            q.applicant_building_number as applicant_address_1,
            q.applicant_thoroughfare as applicant_address_2,
            q.applicant_posttown as applicant_address_3,
            q.applicant_county as applicant_address_4,
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
            on q.pet_breed_code = m.source_breed_id
            and q.quote_source = m.source
    )
select *
from final
