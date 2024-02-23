with

    source as (

        select * from {{ source("src_airbyte", "google_ads_campaign_conversions") }}

    ),

    renamed as (select * from source)

select *
from renamed
