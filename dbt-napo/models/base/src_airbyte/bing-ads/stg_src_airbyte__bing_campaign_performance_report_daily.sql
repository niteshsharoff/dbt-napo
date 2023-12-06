with 

source as (

    select * from {{ source('src_airbyte', 'bing_campaign_performance_report_daily') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        ctr,
        ptr,
        spend,
        clicks,
        assists,
        network,
        revenue,
        deviceos,
        accountid,
        allrevenue,
        averagecpc,
        averagecpm,
        budgetname,
        campaignid,
        devicetype,
        phonecalls,
        timeperiod,
        topvsother,
        accountname,
        adrelevance,
        conversions,
        impressions,
        bidmatchtype,
        budgetstatus,
        campaignname,
        campaigntype,
        currencycode,
        qualityscore,
        costperassist,
        addistribution,
        allconversions,
        campaignlabels,
        campaignstatus,
        conversionrate,
        averageposition,
        returnonadspend,
        customparameters,
        lowqualityclicks,
        phoneimpressions,
        revenueperassist,
        allconversionrate,
        costperconversion,
        allreturnonadspend,
        deliveredmatchtype,
        allcostperconversion,
        conversionsqualified,
        revenueperconversion,
        historicaladrelevance,
        historicalexpectedctr,
        landingpageexperience,
        lowqualityconversions,
        lowqualityimpressions,
        historicalqualityscore,
        viewthroughconversions,
        allrevenueperconversion,
        budgetassociationstatus,
        lowqualityclickspercent,
        lowqualityconversionrate,
        lowqualitysophisticatedclicks,
        historicallandingpageexperience

    from source
    where accountname = '{{var('bing_account_name')}}'

)

select * from renamed
