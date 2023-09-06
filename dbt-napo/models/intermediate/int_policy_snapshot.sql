with
    snapshot_details as (
        select parse_timestamp('%Y-%m-%d', '{{run_started_at.date()}}') as snapshot_at
    )
select *
from {{ ref("int_policy_history") }}, snapshot_details
where
    snapshot_details.snapshot_at >= timestamp(row_effective_from)
    and snapshot_details.snapshot_at < timestamp(row_effective_to)
