{{
    config(
        materialized="table",
        partition_by={
            "field": "created_date",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["daily", "growth"],
    )
}}

with
    data as (
        select
            cast((a.created_at) as date) as created_date,
            case
                (a.source) when 'direct' then 'direct' else pcw_name
            end as quote_source,
            count(quote_id) as total_offered_quotes
        from {{ ref("fct_quote_requests") }} a
        left join
            {{ ref("lookup_quote_pcw_mapping") }} b on a.source = b.quote_code_name
        where state = 'offered' and a.source != 'renewal'
        group by 1, 2
    )
select * replace(lower(quote_source) as quote_source)
from data
