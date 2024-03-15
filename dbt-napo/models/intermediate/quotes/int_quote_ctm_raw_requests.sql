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
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Persons.Person.FirstName'
            ) as applicant_firstname,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Persons.Person.Surname'
            ) as applicant_surname,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Persons.Person.DOB'
            ) as applicant_date_of_birth,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Persons.Person.Email'
            ) as applicant_email,
            json_extract_scalar(
                raw_request,
                '$.QuoteRequest.Lead.RiskMaster.Addresses.Address.AddressLine1'
            ) as applicant_address_line_1,
            json_extract_scalar(
                raw_request,
                '$.QuoteRequest.Lead.RiskMaster.Addresses.Address.AddressLine2'
            ) as applicant_address_line_2,
            json_extract_scalar(
                raw_request,
                '$.QuoteRequest.Lead.RiskMaster.Addresses.Address.AddressLine3'
            ) as applicant_address_line_3,
            json_extract_scalar(
                raw_request,
                '$.QuoteRequest.Lead.RiskMaster.Addresses.Address.AddressLine4'
            ) as applicant_address_line_4,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Addresses.Address.PostCode'
            ) as applicant_postcode,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Name'
            ) as pet_name,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Type'
            ) as pet_species,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Breed'
            ) as pet_breed_code,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Crossbreed'
            ) as pet_is_cross_breed,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.DOB'
            ) as pet_date_of_birth,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Gender'
            ) as pet_gender,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.AmountPaid'
            ) as pet_cost,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Chipped'
            ) as pet_is_microchipped,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Aggressive'
            ) as pet_is_aggressive,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Neutered'
            ) as pet_is_neutered,
            json_extract_scalar(
                raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Vaccinated'
            ) as pet_is_vaccinated
        from {{ ref("stg_raw__quote_request", v=2) }}
        where quote_source = 'comparethemarket'
    ),
    processed_quotes as (
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            initcap(applicant_firstname) as applicant_firstname,
            initcap(applicant_surname) as applicant_surname,
            cast(applicant_date_of_birth as date) as applicant_dob,
            applicant_email,
            applicant_address_line_1,
            applicant_address_line_2,
            applicant_address_line_3,
            applicant_address_line_4,
            applicant_postcode,
            initcap(pet_name) as pet_name,
            lower(pet_species) as pet_species,
            ltrim(pet_breed_code, '0') as pet_breed_code,
            cast(pet_is_cross_breed as bool) as pet_is_cross_breed,
            cast(pet_date_of_birth as date) as pet_dob,
            lower(pet_gender) as pet_gender,
            cast(pet_cost as numeric) as pet_cost,
            cast(pet_is_microchipped as bool) as pet_is_microchipped,
            cast(pet_is_aggressive as bool) as pet_is_aggressive,
            cast(pet_is_neutered as bool) as pet_is_neutered,
            cast(pet_is_vaccinated as bool) as pet_is_vaccinated
        from denormalised_quotes
    ),
    final as (
        -- Breed mapping logic:
        -- https://github.com/CharlieNapo/quote-service/blob/3841792a7151420f2cda7539e3716014c8002602/quote/mapping/compare_the_market.py#L94
        select
            quote_uuid,
            quote_at,
            status,
            quote_source,
            applicant_firstname,
            applicant_surname,
            applicant_dob,
            applicant_email,
            applicant_address_line_1 as applicant_address_1,
            applicant_address_line_2 as applicant_address_2,
            applicant_address_line_3 as applicant_address_3,
            applicant_address_line_4 as applicant_address_4,
            cast(null as string) as applicant_address_5,
            applicant_postcode,
            pet_name,
            pet_dob,
            case
                when pet_gender = 'male'
                then 'male'
                when pet_gender = 'female'
                then 'female'
                else 'unknown'
            end as pet_gender,
            pet_species,
            case
                when
                    pet_is_cross_breed is true
                    and pet_breed_code = '0490'
                    and pet_species = 'cat'
                then 'mixed'
                when pet_is_cross_breed is true and pet_breed_code is not null
                then 'cross'
                when pet_is_cross_breed is true and pet_breed_code is null
                then 'mixed'
                when pet_is_cross_breed is false
                then 'pedigree'
                else null
            end as pet_breed_category,
            m.source_breed_name as pet_source_breed_name,
            m.common_breed_name as pet_common_breed_name,
            pet_is_microchipped,
            pet_is_aggressive,
            pet_is_neutered,
            pet_is_vaccinated
        from processed_quotes q
        left join
            common_breed_map m
            on q.pet_breed_code = m.source_breed_id
            and q.quote_source = m.source
    )
select *
from final
