with 

source as (

    select * from {{ source('src_google_ads', 'ads_AccountBasicStats_4788955894') }}

),

renamed as (

    select
        customer_id,
        metrics_clicks,
        metrics_conversions,
        metrics_conversions_value,
        metrics_cost_micros,
        metrics_impressions,
        metrics_interaction_event_types,
        metrics_interactions,
        metrics_view_through_conversions,
        segments_ad_network_type,
        segments_date,
        segments_device,
        segments_slot,
        _latest_date,
        _data_date

    from source

)

select * from renamed
