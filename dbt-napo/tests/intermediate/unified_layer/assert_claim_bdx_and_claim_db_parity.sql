with
    q1 as (
        select
            claim_bdx.claim_reference as claim_bdx_reference,
            claim_db.claim_reference as claim_db_reference
        from {{ ref("dim_claim_v1") }} claim_bdx
        full outer join
            {{ ref("dim_claim_v2") }} claim_db
            on claim_bdx.claim_reference = claim_db.claim_reference
    )
select distinct *
from q1
where q1.claim_db_reference is null
