{{
    config(
        materialized="incremental",
        partition_by={
            'field':'created_date',
            'data_type':'date',
            'granularity':'day'
        }
    )
}}

select cast((a.created_at) as date) as created_date
        ,b.pcw_name
        ,count(quote_id) as total_offered_quotes 
from {{ref('fct_quote_requests')}} a
left join {{ ref('lookup_quote_pcw_mapping') }} b
on a.source = b.quote_code_name
where state = 'offered'
group by 1,2
