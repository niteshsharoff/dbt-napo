/*
  GIVEN
    a table of policy transactions

  WHEN
    we have a price change MTA to a discounted policy that is more than £1

  THEN
    the discount amount column should never be zero
*/
select
    transaction_type,
    transaction_at,
    policy_number,
    discount_amount,
    gross_premium_ipt_inc
from {{ ref("finance_cumulative_premium_bdx") }}
where
    transaction_type = 'MTA'
    and voucher_code is not null
    and gross_premium_ipt_inc != 0
    and discount_amount = 0
    and abs(gross_premium_ipt_inc) >= 1.0
