with source as (
      select * from {{ source('src_airbyte', 'tiktok_ad_groups_reports_daily') }}
),
renamed as (
    select
        {{ adapter.quote("_airbyte_raw_id") }},
        {{ adapter.quote("_airbyte_extracted_at") }},
        {{ adapter.quote("_airbyte_meta") }},
        {{ adapter.quote("ad_id") }},
        {{ adapter.quote("metrics") }},
        {{ adapter.quote("adgroup_id") }},
        {{ adapter.quote("dimensions") }},
        {{ adapter.quote("campaign_id") }},
        {{ adapter.quote("advertiser_id") }},
        {{ adapter.quote("stat_time_day") }},
        {{ adapter.quote("stat_time_hour") }}

    from source
)
select * from renamed
  