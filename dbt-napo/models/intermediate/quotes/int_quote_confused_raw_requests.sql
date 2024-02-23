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
                raw_request, '$.QuoteRequest.PolicyHolder.Firstname'
            ) as applicant_firstname,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.Surname'
            ) as applicant_surname,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.Email'
            ) as applicant_email,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.DOB'
            ) as applicant_date_of_birth,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.AddressLine1'
            ) as applicant_address_line_1,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.AddressLine2'
            ) as applicant_address_line_2,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.AddressLine3'
            ) as applicant_address_line_3,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.AddressLine4'
            ) as applicant_address_line_4,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.AddressLine5'
            ) as applicant_address_line_5,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.PolicyHolder.PostCode'
            ) as applicant_postcode,
            json_extract_scalar(pet, '$.Name') as pet_name,
            json_extract_scalar(pet, '$.DOB') as pet_date_of_birth,
            json_extract_scalar(pet, '$.Type') as pet_species,
            json_extract_scalar(pet, '$.Breed') as pet_breed,
            json_extract_scalar(pet, '$.BreedCode') as pet_breed_code,
            json_extract_scalar(pet, '$.PurchasePrice') as pet_cost,
            json_extract_scalar(pet, '$.Gender') as pet_gender,
            json_extract_scalar(pet, '$.Microchipped') as pet_is_microchipped,
            json_extract_scalar(pet, '$.Neutered') as pet_is_neutered,
            json_extract_scalar(pet, '$.Vicious') as pet_is_aggressive,
            json_extract_scalar(pet, '$.VaccinationsCurrent') as pet_is_vaccinated
        from {{ ref("stg_raw__quote_request", v=2) }}
        cross join unnest(json_extract_array(raw_request, '$.QuoteRequest.Pet')) as pet
        where quote_source = 'confused'
    ),
    processed_quotes as (
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            initcap(applicant_firstname) as applicant_firstname,
            initcap(applicant_surname) as applicant_surname,
            applicant_email,
            parse_date('%d/%m/%Y', applicant_date_of_birth) as applicant_dob,
            applicant_address_line_1,
            applicant_address_line_2,
            applicant_address_line_3,
            applicant_address_line_4,
            applicant_address_line_5,
            applicant_postcode,
            initcap(pet_name) as pet_name,
            parse_date('%d/%m/%Y', pet_date_of_birth) as pet_dob,
            lower(pet_species) as pet_species,
            pet_breed,
            pet_breed_code,
            cast(replace(pet_cost, ',', '') as numeric) as pet_cost,
            case
                when pet_gender = 'm'
                then 'male'
                when pet_gender = 'f'
                then 'female'
                else 'unknown'
            end as pet_gender,
            case
                when pet_is_microchipped = 'Y' then true else false
            end as pet_is_microchipped,
            case when pet_is_neutered = 'Y' then true else false end as pet_is_neutered,
            case
                when pet_is_aggressive = 'Y' then true else false
            end as pet_is_aggressive,
            case
                when pet_is_vaccinated = 'Y' then true else false
            end as pet_is_vaccinated
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
            q.applicant_address_line_1 as applicant_address_1,
            q.applicant_address_line_2 as applicant_address_2,
            q.applicant_address_line_3 as applicant_address_3,
            q.applicant_address_line_4 as applicant_address_4,
            q.applicant_address_line_5 as applicant_address_5,
            q.applicant_postcode,
            q.pet_name,
            q.pet_dob,
            q.pet_gender,
            q.pet_species,
            -- https://github.com/CharlieNapo/quote-service/blob/main/quote/mapping/confused.py#L26
            case
                when
                    q.pet_breed_code
                    in ('490', '994', '995', '996', '997', '998', '999')
                then 'mixed'
                when m.source_breed_name like '% Cross%'
                then 'cross'
                else 'pedigree'
            end as pet_breed_category,
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
