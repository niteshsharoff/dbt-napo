{{
    config(
        materialized="table",
        partition_by={"field": "date", "data_type": "date", "granularity": "day"},
        tags=["daily", "growth"],
    )
}}

with tiktok_ad_groups as (
  select 
    cast(stat_time_day as date) as date,
    adgroup_id,
    JSON_EXTRACT_SCALAR(metrics, '$.adgroup_name') AS adgroup_name,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.app_install') AS INT64) AS app_install,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.average_video_play') AS FLOAT64) AS average_video_play,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.average_video_play_per_user') AS FLOAT64) AS average_video_play_per_user,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.campaign_id') AS INT64) AS campaign_id,
    JSON_EXTRACT_SCALAR(metrics, '$.campaign_name') AS campaign_name,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.clicks') AS INT64) AS clicks,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.clicks_on_music_disc') AS INT64) AS clicks_on_music_disc,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.comments') AS INT64) AS comments,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.conversion') AS INT64) AS conversion,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.conversion_rate') AS FLOAT64) AS conversion_rate,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.cost_per_1000_reached') AS FLOAT64) AS cost_per_1000_reached,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.cost_per_conversion') AS FLOAT64) AS cost_per_conversion,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.cost_per_result') AS FLOAT64) AS cost_per_result,
    JSON_EXTRACT_SCALAR(metrics, '$.cost_per_secondary_goal_result') AS cost_per_secondary_goal_result,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.cpc') AS FLOAT64) AS cpc,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.cpm') AS FLOAT64) AS cpm,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.ctr') AS FLOAT64) AS ctr,
    JSON_EXTRACT_SCALAR(metrics, '$.dpa_target_audience_type') AS dpa_target_audience_type,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.follows') AS INT64) AS follows,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.frequency') AS FLOAT64) AS frequency,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.impressions') AS INT64) AS impressions,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.likes') AS INT64) AS likes,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.mobile_app_id') AS INT64) AS mobile_app_id,
    JSON_EXTRACT_SCALAR(metrics, '$.placement_type') AS placement_type,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.profile_visits') AS INT64) AS profile_visits,
    JSON_EXTRACT_SCALAR(metrics, '$.promotion_type') AS promotion_type,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.reach') AS INT64) AS reach,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_app_install') AS INT64) AS real_time_app_install,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_app_install_cost') AS FLOAT64) AS real_time_app_install_cost,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_conversion') AS INT64) AS real_time_conversion,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_conversion_rate') AS FLOAT64) AS real_time_conversion_rate,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_cost_per_conversion') AS FLOAT64) AS real_time_cost_per_conversion,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_cost_per_result') AS FLOAT64) AS real_time_cost_per_result,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_result') AS INT64) AS real_time_result,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.real_time_result_rate') AS FLOAT64) AS real_time_result_rate,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.result') AS INT64) AS result,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.result_rate') AS FLOAT64) AS result_rate,
    JSON_EXTRACT_SCALAR(metrics, '$.secondary_goal_result') AS secondary_goal_result,
    JSON_EXTRACT_SCALAR(metrics, '$.secondary_goal_result_rate') AS secondary_goal_result_rate,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.shares') AS INT64) AS shares,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.spend') AS FLOAT64) AS spend,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.tt_app_id') AS INT64) AS tt_app_id,
    JSON_EXTRACT_SCALAR(metrics, '$.tt_app_name') AS tt_app_name,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_play_actions') AS INT64) AS video_play_actions,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_views_p100') AS INT64) AS video_views_p100,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_views_p25') AS INT64) AS video_views_p25,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_views_p50') AS INT64) AS video_views_p50,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_views_p75') AS INT64) AS video_views_p75,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_watched_2s') AS INT64) AS video_watched_2s,
    CAST(JSON_EXTRACT_SCALAR(metrics, '$.video_watched_6s') AS INT64) AS video_watched_6s
  from {{ref('stg_src_airbyte__tiktok_ad_groups_reports_daily')}}
),

agg_ad_groups as (
  select 
        date,
--      campaign_id,
--      campaign_name,
        {{ marketing_campaign_classification("campaign_name") }} as napo_campaign_type,
--      sum(reach) as reach,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversion) as conversions,
        sum(spend) as cost_gbp
  from tiktok_ad_groups
  group by 1,2
)

select *
from agg_ad_groups