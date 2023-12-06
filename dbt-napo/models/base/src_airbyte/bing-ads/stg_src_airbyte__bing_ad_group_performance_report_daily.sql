with 

source as (

    select * from {{ source('src_airbyte', 'bing_ad_group_performance_report_daily') }}

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
        language,
        accountid,
        adgroupid,
        allrevenue,
        averagecpc,
        averagecpm,
        campaignid,
        devicetype,
        phonecalls,
        timeperiod,
        topvsother,
        accountname,
        adgroupname,
        adgrouptype,
        adrelevance,
        conversions,
        expectedctr,
        impressions,
        bidmatchtype,
        campaignname,
        campaigntype,
        currencycode,
        qualityscore,
        costperassist,
        addistribution,
        allconversions,
        conversionrate,
        finalurlsuffix,
        averageposition,
        customparameters,
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
        historicalqualityscore,
        viewthroughconversions,
        allrevenueperconversion,
        historicallandingpageexperience

    from source
        where accountname = '{{var('bing_account_name')}}'

)

select * from renamed
