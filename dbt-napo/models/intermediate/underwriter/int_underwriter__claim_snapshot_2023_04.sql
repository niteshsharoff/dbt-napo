{{ config(materialized="table") }}

select *
from {{ ref("int_underwriter__claim_snapshots") }}
where snapshot_date = date(2023, 5, 9) and claim_received_date < date(2023, 5, 1)
