{{ config(materialized="table", tags=["daily"]) }}

with
    common_breed_map as (
        select *
        from {{ source("raw", "commonbreedmapping") }}
        -- Optimisation: Consider only loading latest partition
        where
            run_date
            = (select max(run_date) from {{ source("raw", "commonbreedmapping") }})
    ),
    raw_requests as (
        select
            q.quote_request_id as quote_uuid,
            timestamp_millis(created_at) as quote_at,
            state as status,
            case
                when
                    source in (
                        'indium-river',
                        'tungsten-vale',
                        'fermium-cliff',
                        'hassium-oxbow',
                        'gallium-rapid'
                    )
                then lower(m.pcw_name)
                else 'direct'
            end as quote_source,
            raw_request
        from {{ source("raw", "quoterequest") }} q
        left join
            {{ ref("lookup_quote_pcw_mapping") }} m on q.source = m.quote_code_name
    ),
    direct_quotes as (
        select q.* except (raw_request, pet), m.common_breed_name as common_breed_name
        from
            (
                select
                    *,
                    json_extract_scalar(pet, '$.name') as pet_name,
                    json_extract_scalar(pet, '$.species') as species,
                    json_extract_scalar(pet, '$.breed_category') as breed_category,
                    json_extract_scalar(pet, '$.breed_name') as source_breed_name
                from raw_requests
                cross join unnest(json_extract_array(raw_request, '$.pets')) as pet
                where
                    quote_source = 'direct'
                    or quote_source = 'renewal'
                    or quote_source = ''
            ) q
        left join
            common_breed_map m
            on q.source_breed_name = m.source_breed_name
            and q.quote_source = m.source
    ),
    ctm_quotes as (
        select
            q.* except (raw_request, breed_code, cross_breed),
            -- Mapping logic:
            -- https://github.com/CharlieNapo/quote-service/blob/3841792a7151420f2cda7539e3716014c8002602/quote/mapping/compare_the_market.py#L94
            case
                when cross_breed is true and breed_code = '0490' and species = 'cat'
                then 'mixed'
                when cross_breed is true and breed_code is not null
                then 'cross'
                when cross_breed is true and breed_code is null
                then 'mixed'
                when cross_breed is false
                then 'pedigree'
                else null
            end as breed_category,
            m.source_breed_name as source_breed_name,
            m.common_breed_name as common_breed_name
        from
            (
                select
                    *,
                    json_extract_scalar(
                        raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Name'
                    ) as pet_name,
                    lower(
                        json_extract_scalar(
                            raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Type'
                        )
                    ) as species,
                    ltrim(
                        json_extract_scalar(
                            raw_request, '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Breed'
                        ),
                        '0'
                    ) as breed_code,
                    cast(
                        json_extract_scalar(
                            raw_request,
                            '$.QuoteRequest.Lead.RiskMaster.Pets.Pet.Crossbreed'
                        ) as bool
                    ) as cross_breed
                from raw_requests
                where quote_source = 'comparethemarket'
            ) q
        left join
            common_breed_map m
            on q.breed_code = m.source_breed_id
            and q.quote_source = m.source
    ),
    -- TODO: Add quotes from other PCWs
    final as (
        select *
        from direct_quotes
        union distinct
        select *
        from ctm_quotes
    )
select *
from final
