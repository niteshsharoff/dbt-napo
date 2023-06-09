{{ config(pre_hook=["{{declare_underwriter_udfs()}}"]) }}

with
  add_cancel_date_to_reinstatements as (
    select *
      -- add cancel date to reinstatements for reinstated premium calculation
      , case
        when transaction_type = 'Reinstatement'
        then lag(policy.cancel_date) over (partition by policy.reference_number order by transaction_at)
        else policy.cancel_date
      end as reinstated_cancel_date
    from {{ ref("int_policy_transaction") }}
  )
  , with_premium_and_retail_price as (
    select *
      , policy.annual_price as retail_price
      , {{ target.schema }}.calculate_premium_price(policy.annual_price, discount.discount_percentage) as premium_price
    from add_cancel_date_to_reinstatements
  )
  , with_discount_and_ipt_amount as (
    select *
      , premium_price - retail_price as discount_amount
      , premium_price - {{target.schema}}.calculate_amount_exc_ipt(premium_price) as ipt_amount
    from with_premium_and_retail_price
  )
  , positions as (
    -- pro-rated amounts for cancellation events
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
      , case
        when transaction_type = 'Cancellation' 
          or transaction_type = 'Cancellation MTA'
        then {{ target.schema }}.calculate_consumed_amount(ipt_amount, policy.start_date, policy.end_date, policy.cancel_date)
        when transaction_type = 'NTU'
        then 0.0
        else ipt_amount
      end as ipt_position
    from with_discount_and_ipt_amount
  )
  , differences as (
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
      , ipt_position - lag(ipt_position, 1, 0) over(
        partition by policy.policy_id order by transaction_at
        , case 
          when transaction_type = 'New Policy' or transaction_type = 'Renewal' then 1
          when transaction_type = 'MTA' then 2
          when transaction_type = 'Cancellation' or transaction_type = 'NTU' then 3
          when transaction_type = 'Cancellation MTA' then 4
          when transaction_type = 'Reinstatement' then 5
          else 6
        end
      ) as ipt_difference
    from positions
  )
  , add_underwriter_dimension as (
    select
      transaction_at
      , transaction_type
      , struct(
        cast(retail_price as numeric) as retail_price
        , cast(ifnull(discount.discount_percentage, 0) as numeric) as discount_percent
        , cast(12 as numeric) as ipt_percent
        , cast(premium_price as numeric) as premium_price_ipt_inc
        , cast({{target.schema}}.calculate_amount_exc_ipt(premium_price) as numeric) as premium_price_ipt_exc
        , cast(discount_amount as numeric) as discount_amount
        , cast(ipt_amount as numeric) as ipt_amount
        , cast(premium_position as numeric) as premium_position_ipt_inc
        , cast({{target.schema}}.calculate_amount_exc_ipt(premium_position) as numeric) as premium_position_ipt_exc
        , cast(discount_position as numeric) as discount_position
        , cast(ipt_position as numeric) as ipt_position
        , cast(premium_difference as numeric) as premium_difference_ipt_inc
        , cast({{target.schema}}.calculate_amount_exc_ipt(premium_difference) as numeric) as premium_difference_ipt_exc
        , cast(discount_difference as numeric) as discount_difference
        , cast(ipt_difference as numeric) as ipt_difference
      ) as underwriter
      , quote
      , policy
      , customer
      , pet
      , product
      , discount
      -- , _audit
    from differences
  )
select *
from add_underwriter_dimension
