{{ config(schema="marts", pre_hook=["{{declare_underwriter_udfs()}}"]) }}

with
    add_cancel_date_to_reinstatements as (
        select
            *,
            -- add cancel date to reinstatements for reinstated premium calculation
            case
                when transaction_type = 'Reinstatement'
                then
                    lag(policy.cancel_date) over (
                        partition by policy.reference_number order by transaction_at
                    )
                else policy.cancel_date
            end as reinstated_cancel_date
        from {{ ref("int_policy_transaction") }}
    ),
    with_premium_and_retail_price as (
        select
            *,
            policy.annual_price as retail_price,
            {{ target.schema }}.calculate_premium_price(
                policy.annual_price, campaign.discount_percentage
            ) as premium_price
        -- TBD: discounts not factored into premium price as of 2023-06-15
        -- , policy.annual_price as premium_price
        from add_cancel_date_to_reinstatements
    ),
    with_discount_and_ipt_amount as (
        select
            *,
            premium_price - retail_price as discount_amount,
            retail_price - {{ target.schema }}.calculate_amount_exc_ipt(
                retail_price
            ) as retail_ipt_amount,
            premium_price - {{ target.schema }}.calculate_amount_exc_ipt(
                premium_price
            ) as premium_ipt_amount
        from with_premium_and_retail_price
    ),
    apply_rounding as (
        -- pre-mature rounding to keep reporting in line with premium_bdx
        select
            * except (
                retail_price,
                premium_price,
                discount_amount,
                retail_ipt_amount,
                premium_ipt_amount
            ),
            round(cast(retail_price as numeric), 2, "ROUND_HALF_EVEN") as retail_price,
            round(cast(premium_price as numeric), 2, "ROUND_HALF_EVEN") as premium_price
            ,
            round(
                cast(discount_amount as numeric), 2, "ROUND_HALF_EVEN"
            ) as discount_amount,
            round(
                cast(retail_ipt_amount as numeric), 2, "ROUND_HALF_EVEN"
            ) as retail_ipt_amount,
            round(
                cast(premium_ipt_amount as numeric), 2, "ROUND_HALF_EVEN"
            ) as premium_ipt_amount
        from with_discount_and_ipt_amount
    ),
    positions as (
        -- pro-rated amounts for cancellation events
        select
            *,
            case
                when
                    transaction_type = 'Cancellation'
                    or transaction_type = 'Cancellation MTA'
                then
                    {{ target.schema }}.calculate_consumed_amount(
                        retail_price,
                        policy.start_date,
                        policy.end_date,
                        policy.cancel_date
                    )
                when transaction_type = 'NTU'
                then 0.0
                else retail_price
            end as retail_position,
            case
                when
                    transaction_type = 'Cancellation'
                    or transaction_type = 'Cancellation MTA'
                then
                    {{ target.schema }}.calculate_consumed_amount(
                        premium_price,
                        policy.start_date,
                        policy.end_date,
                        policy.cancel_date
                    )
                when transaction_type = 'NTU'
                then 0.0
                else premium_price
            end as premium_position,
            case
                when
                    transaction_type = 'Cancellation'
                    or transaction_type = 'Cancellation MTA'
                then
                    {{ target.schema }}.calculate_consumed_amount(
                        discount_amount,
                        policy.start_date,
                        policy.end_date,
                        policy.cancel_date
                    )
                when transaction_type = 'NTU'
                then 0.0
                else discount_amount
            end as discount_position,
            case
                when
                    transaction_type = 'Cancellation'
                    or transaction_type = 'Cancellation MTA'
                then
                    {{ target.schema }}.calculate_consumed_amount(
                        retail_ipt_amount,
                        policy.start_date,
                        policy.end_date,
                        policy.cancel_date
                    )
                when transaction_type = 'NTU'
                then 0.0
                else retail_ipt_amount
            end as retail_ipt_position,
            case
                when
                    transaction_type = 'Cancellation'
                    or transaction_type = 'Cancellation MTA'
                then
                    {{ target.schema }}.calculate_consumed_amount(
                        premium_ipt_amount,
                        policy.start_date,
                        policy.end_date,
                        policy.cancel_date
                    )
                when transaction_type = 'NTU'
                then 0.0
                else premium_ipt_amount
            end as premium_ipt_position
        from apply_rounding
    ),
    differences as (
        -- event ordering when they have the same transaction_at timestamp
        select
            *,
            retail_position - lag(retail_position, 1, 0) over (
                partition by policy.policy_id
                order by
                    transaction_at,
                    case
                        when
                            transaction_type = 'New Policy'
                            or transaction_type = 'Renewal'
                        then 1
                        when transaction_type = 'MTA'
                        then 2
                        when
                            transaction_type = 'Cancellation'
                            or transaction_type = 'NTU'
                        then 3
                        when transaction_type = 'Cancellation MTA'
                        then 4
                        when transaction_type = 'Reinstatement'
                        then 5
                        else 6
                    end
            ) as retail_difference,
            premium_position - lag(premium_position, 1, 0) over (
                partition by policy.policy_id
                order by
                    transaction_at,
                    case
                        when
                            transaction_type = 'New Policy'
                            or transaction_type = 'Renewal'
                        then 1
                        when transaction_type = 'MTA'
                        then 2
                        when
                            transaction_type = 'Cancellation'
                            or transaction_type = 'NTU'
                        then 3
                        when transaction_type = 'Cancellation MTA'
                        then 4
                        when transaction_type = 'Reinstatement'
                        then 5
                        else 6
                    end
            ) as premium_difference,
            discount_position - lag(discount_position, 1, 0) over (
                partition by policy.policy_id
                order by
                    transaction_at,
                    case
                        when
                            transaction_type = 'New Policy'
                            or transaction_type = 'Renewal'
                        then 1
                        when transaction_type = 'MTA'
                        then 2
                        when
                            transaction_type = 'Cancellation'
                            or transaction_type = 'NTU'
                        then 3
                        when transaction_type = 'Cancellation MTA'
                        then 4
                        when transaction_type = 'Reinstatement'
                        then 5
                        else 6
                    end
            ) as discount_difference,
            retail_ipt_position - lag(retail_ipt_position, 1, 0) over (
                partition by policy.policy_id
                order by
                    transaction_at,
                    case
                        when
                            transaction_type = 'New Policy'
                            or transaction_type = 'Renewal'
                        then 1
                        when transaction_type = 'MTA'
                        then 2
                        when
                            transaction_type = 'Cancellation'
                            or transaction_type = 'NTU'
                        then 3
                        when transaction_type = 'Cancellation MTA'
                        then 4
                        when transaction_type = 'Reinstatement'
                        then 5
                        else 6
                    end
            ) as retail_ipt_difference,
            premium_ipt_position - lag(premium_ipt_position, 1, 0) over (
                partition by policy.policy_id
                order by
                    transaction_at,
                    case
                        when
                            transaction_type = 'New Policy'
                            or transaction_type = 'Renewal'
                        then 1
                        when transaction_type = 'MTA'
                        then 2
                        when
                            transaction_type = 'Cancellation'
                            or transaction_type = 'NTU'
                        then 3
                        when transaction_type = 'Cancellation MTA'
                        then 4
                        when transaction_type = 'Reinstatement'
                        then 5
                        else 6
                    end
            ) as premium_ipt_difference
        from positions
    ),
    with_underwriter_dimension as (
        select
            transaction_at,
            transaction_type,
            quote,
            policy,
            customer,
            pet,
            product,
            campaign,
            struct(
                retail_price as retail_price,
                12 as ipt_percent,
                retail_ipt_amount as ipt_amount,
                cast(retail_position as numeric) as premium_position_ipt_inc,
                cast(
                    retail_position - retail_ipt_position as numeric
                ) as premium_position_ipt_exc,
                cast(retail_ipt_position as numeric) as ipt_position,
                cast(retail_difference as numeric) as premium_difference_ipt_inc,
                cast(
                    retail_difference - retail_ipt_difference as numeric
                ) as premium_difference_ipt_exc,
                cast(retail_ipt_difference as numeric) as ipt_difference
            ) as underwriter,
            struct(
                retail_price as retail_price,
                ifnull(campaign.discount_percentage, 0) as discount_percent,
                12 as ipt_percent,
                premium_price as premium_price_ipt_inc,
                discount_amount as discount_amount,
                premium_ipt_amount as ipt_amount,
                cast(premium_position as numeric) as premium_position_ipt_inc,
                cast(
                    premium_position - premium_ipt_position as numeric
                ) as premium_position_ipt_exc,
                cast(discount_position as numeric) as discount_position,
                cast(premium_ipt_position as numeric) as ipt_position,
                cast(premium_difference as numeric) as premium_difference_ipt_inc,
                cast(
                    premium_difference - premium_ipt_difference as numeric
                ) as premium_difference_ipt_exc,
                cast(discount_difference as numeric) as discount_difference,
                cast(premium_ipt_difference as numeric) as ipt_difference
            ) as finance,
            _audit
        from differences
    )
select *
from with_underwriter_dimension
