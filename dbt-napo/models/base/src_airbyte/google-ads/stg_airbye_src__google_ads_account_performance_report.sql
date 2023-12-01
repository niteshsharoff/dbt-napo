with 

source as (

    select * from {{ source('src_airbyte', 'google_ads_account_performance_report') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        customer_id,
        metrics_ctr,
        segments_date,
        segments_week,
        segments_year,
        metrics_clicks,
        segments_month,
        segments_device,
        customer_manager,
        segments_quarter,
        customer_time_zone,
        metrics_average_cpc,
        metrics_average_cpe,
        metrics_average_cpm,
        metrics_average_cpv,
        metrics_conversions,
        metrics_cost_micros,
        metrics_engagements,
        metrics_impressions,
        metrics_video_views,
        metrics_average_cost,
        metrics_interactions,
        segments_day_of_week,
        customer_test_account,
        customer_currency_code,
        metrics_active_view_cpm,
        metrics_active_view_ctr,
        metrics_all_conversions,
        metrics_engagement_rate,
        metrics_video_view_rate,
        metrics_interaction_rate,
        segments_ad_network_type,
        customer_descriptive_name,
        metrics_conversions_value,
        metrics_cost_per_conversion,
        metrics_value_per_conversion,
        customer_auto_tagging_enabled,
        metrics_all_conversions_value,
        metrics_active_view_impressions,
        metrics_active_view_viewability,
        metrics_interaction_event_types,
        metrics_search_impression_share,
        metrics_content_impression_share,
        metrics_cost_per_all_conversions,
        metrics_cross_device_conversions,
        metrics_view_through_conversions,
        metrics_active_view_measurability,
        metrics_value_per_all_conversions,
        metrics_search_rank_lost_impression_share,
        metrics_active_view_measurable_cost_micros,
        metrics_active_view_measurable_impressions,
        metrics_content_rank_lost_impression_share,
        metrics_conversions_from_interactions_rate,
        metrics_search_budget_lost_impression_share,
        metrics_search_exact_match_impression_share,
        metrics_content_budget_lost_impression_share,
        metrics_all_conversions_from_interactions_rate

    from source

)

select * from renamed
