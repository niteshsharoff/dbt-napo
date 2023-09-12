/*
  GIVEN
    a table of policy transactions

  WHEN
    we report the multipet column

  THEN
    GGICE expects the column values to be mapped from true/false -> Yes/No
*/
select transaction_type, transaction_at, policy_number, multipet
from {{ ref("cgice_cumulative_premium_bdx") }}
where multipet != 'Yes' and multipet != 'No'
