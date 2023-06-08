/* 
  GIVEN 
    a list of transactions
    
  WHEN 
    we have an ordered list of transactions
  
  THEN 
    the ordering of transactions should comply to the following
    > No cancellation events (NTU, Cancellation, Cancellation MTA) should precede a MTA transaction
    > No active policy events (New Policy, Renewal, MTA) should precede a Reinstatement transaction
    > No events should precede a New Policy or Renewal transaction
*/
select *
from (
  select policy.reference_number
    , transaction_at
    , transaction_type
    , lag(transaction_type) over(
      partition by policy.policy_id 
      order by transaction_at
      , case 
        when transaction_type = 'New Policy' or transaction_type = 'Renewal' then 1
        when transaction_type = 'MTA' then 2
        when transaction_type = 'Cancellation' or transaction_type = 'NTU' then 3
        when transaction_type = 'Cancellation MTA' then 4
        when transaction_type = 'Reinstatement' then 5
        else 6
      end
    ) as prev_transaction_type
  from {{ref('fct_policy_transaction')}}
)
where (
  -- expect no cancellation events should precede MTA
  transaction_type = 'MTA' and (
    prev_transaction_type = 'NTU'
    or prev_transaction_type = 'Cancellation'
    or prev_transaction_type = 'Cancellation MTA'
  )
) or (
  -- expect no active policy events should precede reinstatement
  transaction_type = 'Reinstatement' and (
    prev_transaction_type = 'New Policy'
    or prev_transaction_type = 'Renewal'
    or prev_transaction_type = 'MTA'
  )
) or (
  -- expect no event should precede new policy or renewal
  (transaction_type = 'New Policy' or transaction_type = 'Renewal')
  and prev_transaction_type is not null
)