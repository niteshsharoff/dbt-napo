with voucher as (
  select quote_id
    , id as voucher_id
    , code
    , discount_percentage
    , affiliate_channel
    , created_date
    , run_date
  from {{ ref('stg_raw__vouchercode') }}
)
, discounts as (
  select * from voucher
)
select *
from discounts

