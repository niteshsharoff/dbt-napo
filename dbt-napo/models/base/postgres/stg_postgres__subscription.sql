select * except (policyid, pk), policyid as policy_id, pk as subscription_id
from {{ source("postgres", "subscription") }}
