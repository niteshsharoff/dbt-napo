select policy.reference_number
from {{ ref("dim_policy_claim_snapshot") }}
where
    policy.reference_number not in (
        select distinct (policy.reference_number)
        from {{ ref("reporting_policy_transaction") }}
    )
    and policy_status <> 'not_purchased'
