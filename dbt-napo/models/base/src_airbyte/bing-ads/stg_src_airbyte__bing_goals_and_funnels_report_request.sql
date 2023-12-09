with 

source as (

    select * from {{ source('src_airbyte', 'bing_goals_and_funnels_report_request') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        accountid,
        accountname,
        cast(timeperiod as date) as timeperiod,
        goal,
        goaltype,
        cast(allrevenue as numeric) as allrevenue,
        campaignid,
        campaignname,
        accountnumber,
        cast(allconversions as numeric) as allconversions,
        viewthroughrevenue,
        cast(allconversionsqualified as numeric) as allconversionsqualified,
        viewthroughconversionsqualified

    from source

)

select * from renamed
