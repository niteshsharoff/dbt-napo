with recursive
  renewed_policy_ids AS (
    select distinct(new_policy_id)
    from {{ ref("stg_raw__renewal") }}
  )
  , find_original_policy_id as (
    select new_policy_id, old_policy_id, 1 as policy_year
    from {{ ref("stg_raw__renewal") }}
    where new_policy_id in (select new_policy_id from renewed_policy_ids)
    union all
    select o.new_policy_id, r.old_policy_id, o.policy_year + 1 as policy_year
    from {{ ref("stg_raw__renewal") }} r
    join find_original_policy_id o
    on r.new_policy_id = o.old_policy_id
  )
  , with_max_policy_year as (
    select new_policy_id
      , old_policy_id
      , p.quote_source
      , policy_year
      , max(policy_year) over(partition by new_policy_id) as max_policy_year
    from find_original_policy_id r
    left join raw.policy p on p.policy_id = r.old_policy_id
  )
select distinct 
  new_policy_id as policy_id
  , policy_year
  , old_policy_id as original_policy_id
  , quote_source as original_quote_source
from with_max_policy_year
where policy_year = max_policy_year
