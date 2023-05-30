select id as voucher_id
  , code as voucher_code
  , discount_percentage
  , quote_id
  , redemption_id
  , affiliate_channel
  , timestamp_millis(created_date) as created_date
from {{ source('raw', 'vouchercode') }}