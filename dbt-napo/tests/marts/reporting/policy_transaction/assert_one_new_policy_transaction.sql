/*
  GIVEN
    a table of policy transactions

  WHEN
    we count the number of new policies or renewal transactions

  THEN
    we expect the row count to be <= 1
*/
select policy.reference_number, transaction_type, count(*) as row_count
from {{ ref("fct_policy_transaction") }}
group by policy.reference_number, transaction_type
having
    (transaction_type = 'New Policy' or transaction_type = 'Renewal') and row_count > 1
