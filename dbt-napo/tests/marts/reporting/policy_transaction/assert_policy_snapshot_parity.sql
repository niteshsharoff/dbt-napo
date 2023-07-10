/*
  GIVEN
    a table of policy transactions and a snapshot of policy transactions

  WHEN
    we compare both tables

  THEN
    we expect the number of distinct policy reference numbers to be aligned
    across both tables
*/
select policy_reference_number
from {{ ref("int_underwriter__policy_snapshot") }}
where
    policy_reference_number not in (
        select distinct policy.reference_number
        from {{ ref("reporting_policy_transaction") }}
    )
