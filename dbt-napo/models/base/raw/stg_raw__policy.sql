with raw as (
    select * except(start_date,end_date,created_date,accident_cover_start_date,illness_cover_start_date,change_at,effective_at,cancel_date)
            ,cast(timestamp_millis(start_date) as date) as start_date
            ,cast(timestamp_millis(end_date) as date) as end_date
            ,cast(timestamp_millis(created_date) as date) as created_date
            ,cast(timestamp_millis(cancel_date) as date) as cancel_date
            ,cast(timestamp_millis(accident_cover_start_date) as date) as accident_cover_start_date 
            ,cast(timestamp_millis(illness_cover_start_date) as date) as illness_cover_start_date
            ,timestamp_millis(change_at) as change_at
            ,cast(timestamp_millis(effective_at) as date) as effective_at
            ,row_number() over(partition by policy_id order by version_id desc) as row_no
    from {{source('raw','policy')}}
)

select * except(row_no)
from raw
where row_no = 1