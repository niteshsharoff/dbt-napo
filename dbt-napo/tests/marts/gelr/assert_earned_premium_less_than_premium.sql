select snapshot_date, policy.reference_number
from {{ ref("dim_policy_claim_snapshot") }}
where earned_premium_ipt_inc > premium_price_ipt_inc
