with 

source as (

    select * from {{ source('src_airbyte', 'bing_account_performance_report_daily') }}

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
        averagecpc,
        averagecpm,
        devicetype,
        phonecalls,
        timeperiod,
        topvsother,
        accountname,
        conversions,
        impressions,
        bidmatchtype,
        currencycode,
        accountnumber,
        costperassist,
        addistribution,
        conversionrate,
        averageposition,
        returnonadspend,
        lowqualityclicks,
        phoneimpressions,
        revenueperassist,
        costperconversion,
        deliveredmatchtype,
        conversionsqualified,
        revenueperconversion,
        lowqualityconversions,
        lowqualityimpressions,
        lowqualityclickspercent,
        lowqualityconversionrate,
        lowqualitysophisticatedclicks

    from source
    where accountnumber = 'F149C4W5'

)

select * from renamed
