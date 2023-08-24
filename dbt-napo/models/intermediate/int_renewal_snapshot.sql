with
    snapshot_details as (
        select parse_date('%Y-%m-%d', '{{run_started_at.date()}}') as snapshot_date
    ),
    policy_claims as (
        select claim.policy_id, array_agg(claim) as claims
        from {{ ref("int_claim_snapshot") }} as claim
        group by claim.policy_id
    )
select
    renewal.*,
    snapshot_details.*,
    coalesce(old_policy_claims.claims, []) as old_policy_claims,
    coalesce(new_policy_claims.claims, []) as new_policy_claims
from {{ ref("int_renewal_history") }} as renewal, snapshot_details
left join
    policy_claims as old_policy_claims
    on old_policy_claims.policy_id = old_policy.policy_id
left join
    policy_claims as new_policy_claims
    on new_policy_claims.policy_id = new_policy.policy_id
where
    renewal.row_effective_from <= timestamp(snapshot_details.snapshot_date)
    and timestamp(snapshot_details.snapshot_date) < renewal.row_effective_to
