with 

source as (

    select * from {{ source('src_google_ads', 'ads_AdGroup_4788955894') }}

),

renamed as (

    select
        ad_group_id,
        campaign_id,
        customer_id,
        ad_group_ad_rotation_mode,
        ad_group_cpc_bid_micros,
        ad_group_cpm_bid_micros,
        ad_group_cpv_bid_micros,
        ad_group_display_custom_bid_dimension,
        ad_group_effective_target_cpa_micros,
        ad_group_effective_target_cpa_source,
        ad_group_effective_target_roas,
        ad_group_effective_target_roas_source,
        ad_group_name,
        ad_group_status,
        ad_group_tracking_url_template,
        ad_group_type,
        ad_group_url_custom_parameters,
        campaign_bidding_strategy,
        campaign_bidding_strategy_type,
        campaign_manual_cpc_enhanced_cpc_enabled,
        campaign_percent_cpc_enhanced_cpc_enabled,
        _latest_date,
        _data_date

    from source

)

select * from renamed
