select policy_id, quote_source
from `raw.policy` q1
where policy_id in (
  select old_policy_id from `raw.renewal`
)
group by policy_id, quote_source