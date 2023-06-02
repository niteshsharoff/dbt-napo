{{ config(pre_hook=["{{declare_underwriter_udfs()}}"]) }}

with
  add_cancel_date_to_reinstatements as (
    select *
      -- Calculate premium price
      , {{ target.schema }}.calculate_premium_price(
          policy.annual_price, discount.discount_percentage
      ) as premium_price
      -- Discount amount
      , {{ target.schema }}.calculate_premium_price(
          policy.annual_price, discount.discount_percentage
      ) - policy.annual_price as discount_amount
      -- Get cancel date from previous cancellation row for premium calculation
      , case
        when transaction_type = 'Reinstatement'
        then lag(policy.cancel_date) over (partition by policy.reference_number order by transaction_at)
        else policy.cancel_date
      end as reinstated_cancel_date
    from {{ ref("int_policy_transaction") }}
  )
  , calculate_premium_position as (
    select *
      -- Calculate premium price value based on transaction type
      , case
        when transaction_type = 'Cancellation' 
          or transaction_type = 'Cancel Reinstatement'
          or transaction_type = 'Cancellation MTA'
        then {{ target.schema }}.calculate_consumed_amount(premium_price, policy.start_date, policy.end_date, policy.cancel_date)
        else premium_price
      end as premium_position
      , case
        when transaction_type = 'Cancellation' 
          or transaction_type = 'Cancel Reinstatement'
          or transaction_type = 'Cancellation MTA'
        then {{ target.schema }}.calculate_consumed_amount(discount_amount, policy.start_date, policy.end_date, policy.cancel_date)
        else discount_amount
      end as discount_position
    from add_cancel_date_to_reinstatements
  )
  , calculate_differences as (
    select *
      , premium_position - lag(premium_position, 1, 0) over(
        partition by policy.policy_id 
        order by transaction_at
        , case 
          when transaction_type = 'New Policy' or transaction_type = 'Renewal'
          then 1
          when transaction_type = 'MTA'
          then 2
          when transaction_type ='Cancellation'
          then 3
          when transaction_type = 'Cancellation MTA'
          then 4
          when transaction_type = 'Reinstatement'
          then 5
          else 6
        end
      ) as premium_difference
      , discount_position - lag(discount_position, 1, 0) over(
        partition by policy.policy_id order by transaction_at
        , case 
          when transaction_type = 'New Policy' or transaction_type = 'Renewal'
          then 1
          when transaction_type = 'MTA'
          then 2
          when transaction_type ='Cancellation'
          then 3
          when transaction_type = 'Cancellation MTA'
          then 4
          when transaction_type = 'Reinstatement'
          then 5
          else 6
        end
      ) as discount_difference
    from calculate_premium_position
  )
  , final as (
    select
      transaction_at,
      transaction_type,
      round(premium_price, 2) as premium_price,
      round(discount_amount, 2) as discount_amount,
      policy.annual_price as retail_price,
      round(premium_position, 2) as premium_position,
      round(discount_position, 2) as discount_position, 
      round(premium_difference, 2) as premium_difference,
      round(discount_difference, 2) as discount_difference,
      policy,
      customer,
      pet
    from calculate_differences
  )
select *
from final
