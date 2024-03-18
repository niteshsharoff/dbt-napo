{{
    config(
        materialized="table",
        partition_by={"field": "date", "data_type": "date", "granularity": "day"},
        tags=["daily", "growth"],
    )
}}

with
    tiktok_ad_groups as (
        select
            cast(stat_time_day as date) as date,
            adgroup_id,
            json_extract_scalar(metrics, '$.adgroup_name') as adgroup_name,
            cast(json_extract_scalar(metrics, '$.app_install') as int64) as app_install,
            cast(
                json_extract_scalar(metrics, '$.average_video_play') as float64
            ) as average_video_play,
            cast(
                json_extract_scalar(metrics, '$.average_video_play_per_user') as float64
            ) as average_video_play_per_user,
            cast(json_extract_scalar(metrics, '$.campaign_id') as int64) as campaign_id,
            json_extract_scalar(metrics, '$.campaign_name') as campaign_name,
            cast(json_extract_scalar(metrics, '$.clicks') as int64) as clicks,
            cast(
                json_extract_scalar(metrics, '$.clicks_on_music_disc') as int64
            ) as clicks_on_music_disc,
            cast(json_extract_scalar(metrics, '$.comments') as int64) as comments,
            cast(json_extract_scalar(metrics, '$.conversion') as int64) as conversion,
            cast(
                json_extract_scalar(metrics, '$.conversion_rate') as float64
            ) as conversion_rate,
            cast(
                json_extract_scalar(metrics, '$.cost_per_1000_reached') as float64
            ) as cost_per_1000_reached,
            cast(
                json_extract_scalar(metrics, '$.cost_per_conversion') as float64
            ) as cost_per_conversion,
            cast(
                json_extract_scalar(metrics, '$.cost_per_result') as float64
            ) as cost_per_result,
            json_extract_scalar(
                metrics, '$.cost_per_secondary_goal_result'
            ) as cost_per_secondary_goal_result,
            cast(json_extract_scalar(metrics, '$.cpc') as float64) as cpc,
            cast(json_extract_scalar(metrics, '$.cpm') as float64) as cpm,
            cast(json_extract_scalar(metrics, '$.ctr') as float64) as ctr,
            json_extract_scalar(
                metrics, '$.dpa_target_audience_type'
            ) as dpa_target_audience_type,
            cast(json_extract_scalar(metrics, '$.follows') as int64) as follows,
            cast(json_extract_scalar(metrics, '$.frequency') as float64) as frequency,
            cast(json_extract_scalar(metrics, '$.impressions') as int64) as impressions,
            cast(json_extract_scalar(metrics, '$.likes') as int64) as likes,
            cast(
                json_extract_scalar(metrics, '$.mobile_app_id') as int64
            ) as mobile_app_id,
            json_extract_scalar(metrics, '$.placement_type') as placement_type,
            cast(
                json_extract_scalar(metrics, '$.profile_visits') as int64
            ) as profile_visits,
            json_extract_scalar(metrics, '$.promotion_type') as promotion_type,
            cast(json_extract_scalar(metrics, '$.reach') as int64) as reach,
            cast(
                json_extract_scalar(metrics, '$.real_time_app_install') as int64
            ) as real_time_app_install,
            cast(
                json_extract_scalar(metrics, '$.real_time_app_install_cost') as float64
            ) as real_time_app_install_cost,
            cast(
                json_extract_scalar(metrics, '$.real_time_conversion') as int64
            ) as real_time_conversion,
            cast(
                json_extract_scalar(metrics, '$.real_time_conversion_rate') as float64
            ) as real_time_conversion_rate,
            cast(
                json_extract_scalar(
                    metrics, '$.real_time_cost_per_conversion'
                ) as float64
            ) as real_time_cost_per_conversion,
            cast(
                json_extract_scalar(metrics, '$.real_time_cost_per_result') as float64
            ) as real_time_cost_per_result,
            cast(
                json_extract_scalar(metrics, '$.real_time_result') as int64
            ) as real_time_result,
            cast(
                json_extract_scalar(metrics, '$.real_time_result_rate') as float64
            ) as real_time_result_rate,
            cast(json_extract_scalar(metrics, '$.result') as int64) as result,
            cast(
                json_extract_scalar(metrics, '$.result_rate') as float64
            ) as result_rate,
            json_extract_scalar(
                metrics, '$.secondary_goal_result'
            ) as secondary_goal_result,
            json_extract_scalar(
                metrics, '$.secondary_goal_result_rate'
            ) as secondary_goal_result_rate,
            cast(json_extract_scalar(metrics, '$.shares') as int64) as shares,
            cast(json_extract_scalar(metrics, '$.spend') as float64) as spend,
            cast(json_extract_scalar(metrics, '$.tt_app_id') as int64) as tt_app_id,
            json_extract_scalar(metrics, '$.tt_app_name') as tt_app_name,
            cast(
                json_extract_scalar(metrics, '$.video_play_actions') as int64
            ) as video_play_actions,
            cast(
                json_extract_scalar(metrics, '$.video_views_p100') as int64
            ) as video_views_p100,
            cast(
                json_extract_scalar(metrics, '$.video_views_p25') as int64
            ) as video_views_p25,
            cast(
                json_extract_scalar(metrics, '$.video_views_p50') as int64
            ) as video_views_p50,
            cast(
                json_extract_scalar(metrics, '$.video_views_p75') as int64
            ) as video_views_p75,
            cast(
                json_extract_scalar(metrics, '$.video_watched_2s') as int64
            ) as video_watched_2s,
            cast(
                json_extract_scalar(metrics, '$.video_watched_6s') as int64
            ) as video_watched_6s
        from {{ ref("stg_src_airbyte__tiktok_ad_groups_reports_daily") }}
    ),

    agg_ad_groups as (
        select
            date,
            -- campaign_id,
            -- campaign_name,
            {{ marketing_campaign_classification("campaign_name") }}
            as napo_campaign_type,
            -- sum(reach) as reach,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(conversion) as conversions,
            sum(spend) as cost_gbp
        from tiktok_ad_groups
        group by 1, 2
    )

select *
from agg_ad_groups
