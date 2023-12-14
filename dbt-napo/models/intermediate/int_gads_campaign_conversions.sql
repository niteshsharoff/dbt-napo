-- Inline CTE for better optimization
SELECT 
    segments_date as date
        ,case 
        when lower(campaign_name) like '%leadgen%' then 'leadgen'
        when lower(campaign_name) not like any ('%leadgen%','%standalone%') then 'growth'
        when lower(campaign_name) like '%standalone%' then 'pa_standalone'
        else 'other'
    end as napo_campaign_type
    ,case
      when lower(segments_ad_network_type) like '%youtube%' then true
      else false
    end as is_youtube_campaign
    ,SUM(IF(cast(split(segments_conversion_action,'conversionActions/')[safe_offset(1)] as string) = '820242956', cast(metrics_conversions as numeric), NULL)) as view_quote_conversions
    ,SUM(IF(cast(split(segments_conversion_action,'conversionActions/')[safe_offset(1)] as string) = '820238111', cast(metrics_conversions as numeric), NULL)) as purchase_conversions
    ,SUM(IF(cast(split(segments_conversion_action,'conversionActions/')[safe_offset(1)] as string) = '6507662370', cast(metrics_conversions as numeric), NULL)) as lead_conversions
    ,SUM(IF(cast(split(segments_conversion_action,'conversionActions/')[safe_offset(1)] as string) = '6676654061', cast(metrics_conversions as numeric), NULL)) as academy_registration_conversions
    ,SUM(cast(metrics_conversions_value as numeric)) as all_conversions_value
FROM {{ref('stg_src_airbyte__google_ads_campaign_conversions')}}
--WHERE segments_date = '2023-12-06'
GROUP BY 1,2,3
