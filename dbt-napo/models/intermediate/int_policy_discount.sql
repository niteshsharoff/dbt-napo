with voucher as (
  select quote_id
    , id as voucher_id
    , code
    , discount_percentage
    , 0 as reward
    , affiliate_channel
    , created_date
    , null as lockin_period_days
    , cast(null as timestamp) as expires_after
    , run_date
  from {{ ref('stg_raw__vouchercode') }}
)
, napo_benefit as (
  select quote_id
    , '' as voucher_id
    , code
    , 0 discount_percentage
    , reward
    , affiliate_channel
    , q.created_date
    , lockin_period_days
    , expires_after
    , q.run_date
  from {{ ref('stg_raw__quotewithbenefit') }} q
  join {{ ref('stg_raw__napobenefitcode') }} b on q.benefit_id = b.id
)
, discounts as (
  select * from voucher
  union all 
  select * from napo_benefit
)
select *
from discounts

