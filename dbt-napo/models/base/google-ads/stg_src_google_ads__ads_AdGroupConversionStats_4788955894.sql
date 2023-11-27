with 

source as (

    select * from {{ source('src_google_ads', 'ads_AdGroupConversionStats_4788955894') }}

),

renamed as (

    select
        ad_group_id,
        campaign_id,
        customer_id,
        ad_group_base_ad_group,
        campaign_base_campaign,
        metrics_conversions,
        metrics_conversions_value,
        metrics_value_per_conversion,
        segments_ad_network_type,
        segments_click_type,
        segments_conversion_action,
        segments_conversion_action_category,
        segments_conversion_action_name,
        segments_date,
        segments_day_of_week,
        segments_device,
        segments_month,
        segments_quarter,
        segments_slot,
        segments_week,
        segments_year,
        _latest_date,
        _data_date

    from source

)

select * from renamed
