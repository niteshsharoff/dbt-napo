{{ config(pre_hook=["{{declare_underwriter_udfs()}}"]) }}

with
  calculate_premium_price as (
    select *
      -- premium price
      , {{ target.schema }}.calculate_premium_price(
          policy.annual_price, discount.discount_percentage
      ) as premium_price
      -- discount amount = premium price - retail price
      , {{ target.schema }}.calculate_premium_price(
          policy.annual_price, discount.discount_percentage
      ) - policy.annual_price as discount_amount
      -- get cancel date for premium calculation
      , case
        when transaction_type = 'Reinstatement'
        then lag(policy.cancel_date) over (partition by policy.reference_number order by transaction_at)
        else policy.cancel_date
      end as reinstated_cancel_date
    from {{ ref("int_policy_transaction") }}
  )
  , calculate_premium_position as (
    -- pro-rated consumed premium and discount amounts for cancellation events
    select *
      , case
        when transaction_type = 'Cancellation' 
          or transaction_type = 'Cancellation MTA'
        then {{ target.schema }}.calculate_consumed_amount(premium_price, policy.start_date, policy.end_date, policy.cancel_date)
        when transaction_type = 'NTU'
        then 0.0
        else premium_price
      end as premium_position
      , case
        when transaction_type = 'Cancellation' 
          or transaction_type = 'Cancellation MTA'
        then {{ target.schema }}.calculate_consumed_amount(discount_amount, policy.start_date, policy.end_date, policy.cancel_date)
        when transaction_type = 'NTU'
        then 0.0
        else discount_amount
      end as discount_position
    from calculate_premium_price
  )
  , calculate_differences as (
    -- event ordering when they have the same transaction_at timestamp
    select *
      , premium_position - lag(premium_position, 1, 0) over(
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
      ) as premium_difference
      , discount_position - lag(discount_position, 1, 0) over(
        partition by policy.policy_id order by transaction_at
        , case 
          when transaction_type = 'New Policy' or transaction_type = 'Renewal' then 1
          when transaction_type = 'MTA' then 2
          when transaction_type = 'Cancellation' or transaction_type = 'NTU' then 3
          when transaction_type = 'Cancellation MTA' then 4
          when transaction_type = 'Reinstatement' then 5
          else 6
        end
      ) as discount_difference
    from calculate_premium_position
  )
  , add_underwriter_dimension as (
    select
      transaction_at
      , transaction_type
      , struct(
        cast(round(premium_price, 2) as numeric) as premium_price,
        cast(round(discount_amount, 2) as numeric) as discount_amount,
        cast(round(premium_position, 2) as numeric) as premium_position,
        cast(round(discount_position, 2) as numeric) as discount_position,
        cast(round(premium_difference, 2) as numeric) as premium_difference,
        cast(round(discount_difference, 2) as numeric) as discount_difference
      ) as underwriter
      , quote
      , policy
      , customer
      , pet
      , product
      , discount
      -- , _audit
    from calculate_differences
  )
select *
from add_underwriter_dimension
