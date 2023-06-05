select policy.reference_number
  , sum(premium_difference) as cumulative_premium_position
from {{ref('int_underwriter__policy_transaction')}}
group by policy.reference_number
having cumulative_premium_position < 0.0