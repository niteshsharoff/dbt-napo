{{ config(tags=["daily", "growth"]) }}

select
    segments_date as date,
    case
        when lower(campaign_name) like '%leadgen%'
        then 'leadgen'
        when lower(campaign_name) not like any ('%leadgen%', '%standalone%')
        then 'growth'
        when lower(campaign_name) like '%standalone%'
        then 'pa_standalone'
        else 'other'
    end as napo_campaign_type,
    case
        when lower(segments_ad_network_type) like '%youtube%' then true else false
    end as is_youtube_campaign,
    sum(
        if(
            cast(
                split(segments_conversion_action, 'conversionActions/')[
                    safe_offset(1)
                ] as string
            )
            = '820242956',
            cast(metrics_conversions as numeric),
            null
        )
    ) as view_quote_conversions,
    sum(
        if(
            cast(
                split(segments_conversion_action, 'conversionActions/')[
                    safe_offset(1)
                ] as string
            )
            = '820238111',
            cast(metrics_conversions as numeric),
            null
        )
    ) as purchase_conversions,
    sum(
        if(
            cast(
                split(segments_conversion_action, 'conversionActions/')[
                    safe_offset(1)
                ] as string
            )
            = '6507662370',
            cast(metrics_conversions as numeric),
            null
        )
    ) as lead_conversions,
    sum(
        if(
            cast(
                split(segments_conversion_action, 'conversionActions/')[
                    safe_offset(1)
                ] as string
            )
            = '6676654061',
            cast(metrics_conversions as numeric),
            null
        )
    ) as academy_registration_conversions,
    sum(cast(metrics_conversions_value as numeric)) as all_conversions_value
from {{ ref("stg_src_airbyte__google_ads_campaign_conversions") }}
-- WHERE segments_date = '2023-12-06'
group by 1, 2, 3
