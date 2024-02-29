{{ config(severity="warn") }}

/*
  GIVEN
    a log of transactions from claim-service and a set of historical claim_bdxs
  WHEN
    we join them together on claim_reference
  THEN
    we expect no claims in the claim_bdx to be missing from the claim-service DB
*/
with
    diff as (
        select
            claim_bdx.claim_reference as claim_bdx_reference,
            claim_db.claim_reference as claim_db_reference
        from {{ ref("dim_claim", v=1) }} claim_bdx
        full outer join
            {{ ref("dim_claim", v=2) }} claim_db
            on claim_bdx.claim_reference = claim_db.claim_reference
    )
select distinct *
from diff
where diff.claim_db_reference is null
