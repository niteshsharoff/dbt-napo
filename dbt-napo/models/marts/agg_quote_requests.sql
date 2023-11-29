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

select cast((created_at) as date) as created_date
        ,count(quote_id) as total_offered_quotes 
from {{ref('fct_quote_requests')}}
--tablesample system(1 percent)
where state = 'offered'
group by 1
order by 1 desc
