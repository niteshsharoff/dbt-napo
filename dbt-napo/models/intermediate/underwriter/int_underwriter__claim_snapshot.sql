with
    snapshot_details as (
        select parse_date('%Y-%m-%d', '{{run_started_at.date()}}') as snapshot_date
    )
select claim.*
from {{ ref("int_underwriter__claim_snapshots") }} as claim, snapshot_details
where claim.snapshot_date = snapshot_details.snapshot_date
