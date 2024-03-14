with
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
                else source
            end as quote_source,
            raw_request
        from {{ source("raw", "quoterequest") }} q
        left join
            {{ ref("lookup_quote_pcw_mapping") }} m on q.source = m.quote_code_name
    )
select *
from raw_requests
