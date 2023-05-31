{{ config(materialized='table') }}
SELECT
    *
FROM
    {{ ref("int_quote") }}
WHERE
    quote_id IN (select distinct quote_id from {{ref("stg_raw__policy")}})
