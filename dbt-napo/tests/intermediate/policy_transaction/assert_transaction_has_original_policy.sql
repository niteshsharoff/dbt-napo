/* 
  GIVEN 
    a table of policy transactions
    
  WHEN 
    we have MTA, Cancellations or Reinstatmenets transactions
  
  THEN 
    they should all be attributable to a new policy or renewal
*/
-- with new_policies as (
--   select distinct(policy_reference_number) as reference_number
--   from {{ref('policy_transaction_fixtures')}}
--   where transaction_type = 'New Policy' or transaction_type = 'Renewal'
-- )
-- select *
-- from {{ref('policy_transaction_fixtures')}}
-- where policy_reference_number not in (select reference_number from new_policies)