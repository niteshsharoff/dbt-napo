select *
from {{ ref("int_claim_snapshots") }}
where snapshot_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
