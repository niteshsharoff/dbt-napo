with 

source as (

    select * from {{ source('src_airbyte', 'conectia_daily_summary') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        epc,
        clicks,
        date_dt,
        revenue,
        currency,
        conversions,
        impressions,
        conversion_rate

    from source

)

select * from renamed
