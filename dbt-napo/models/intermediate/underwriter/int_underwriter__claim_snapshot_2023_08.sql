{{ config(materialized="table") }}

select *
from {{ ref("int_underwriter__claim_snapshots") }}
where snapshot_date = date(2023, 9, 1) and claim_received_date < date(2023, 9, 1)
