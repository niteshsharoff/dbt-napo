{{ config(severity="warn") }}
/*
  GIVEN
    a table of policy transactions

  THEN
    we expect the original quote source to be populated
*/
select
    transaction_type, transaction_at, policy_number, original_quote_source, quote_source
from {{ ref("cgice_cumulative_premium_bdx") }}
where
    original_quote_source is null
    or quote_source = 'renewal'
    and original_quote_source is null
