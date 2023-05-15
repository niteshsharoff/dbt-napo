{{ config(pre_hook=["{{declare_policy_udfs()}}"]) }}
with
    extended_ledger as (
        select
            * except(sold, prev_sold, cancel, prev_cancel),
            sum(
                case 
                    when sold = true and (prev_sold = false or prev_sold is null) 
                    then 1 
                    else 0 
                end
            ) over (partition by policy_id order by change_at) as _sold_grp,
            sum(
                case 
                    when cancel = true and prev_cancel = false 
                    then 1 
                    else 0 
                end
            ) over (partition by policy_id order by change_at) as _cancel_grp,
            sum(
                case 
                    when cancel = false and prev_cancel = true 
                    then 1 
                    else 0 
                end
            ) over (partition by policy_id order by change_at) as _reinstate_grp
        from
            (
                select
                    *,
                    dbt_jeremiah.is_sold(annual_payment_id, subscription_id) as sold,
                    lead(dbt_jeremiah.is_sold(annual_payment_id, subscription_id)) over (
                        partition by policy_id order by change_at desc
                    ) as prev_sold,
                    dbt_jeremiah.is_cancelled(annual_payment_id, subscription_id, cancel_date) as cancel,
                    lead(dbt_jeremiah.is_cancelled(annual_payment_id, subscription_id, cancel_date)) over (
                        partition by policy_id order by change_at desc
                    ) as prev_cancel
                from raw.policy
            )
        order by version_id desc
    ),
    policy_ledger as (
        select
            * except (
                start_date,
                end_date,
                created_date,
                cancel_date,
                accident_cover_start_date,
                illness_cover_start_date,
                change_at,
                effective_at,
                _sold_grp,
                _cancel_grp,
                _reinstate_grp
            ),
            extract(date from timestamp_millis(start_date)) as start_date,
            extract(date from timestamp_millis(end_date)) as end_date,
            extract(date from timestamp_millis(created_date)) as created_date,
            extract(date from timestamp_millis(cancel_date)) as cancel_date,
            extract(date from timestamp_millis(accident_cover_start_date)) as accident_cover_start_date,
            extract(date from timestamp_millis(illness_cover_start_date)) as illness_cover_start_date,
            timestamp_millis(change_at) as change_at,
            -- find the transaction time for when a policy is first sold
            first_value(
                case
                    when dbt_jeremiah.is_sold(annual_payment_id, subscription_id)
                    then timestamp_millis(effective_at)
                    else null
                end
            ) over (partition by policy_id, _sold_grp order by change_at) as sold_at,
            -- find the transaction time for when a policy is last cancelled
            first_value(
                case
                    when dbt_jeremiah.is_cancelled(annual_payment_id, subscription_id, cancel_date)
                    then timestamp_millis(effective_at)
                    else null
                end
            ) over (partition by policy_id, _cancel_grp order by change_at) as cancelled_at,
            -- find the transaction time for when a policy is last reinstated
            first_value(
                case 
                    when cancel_date is null and _reinstate_grp > 0 
                    then timestamp_millis(effective_at) 
                    else null 
                end
            ) over (partition by policy_id, _reinstate_grp order by change_at) as reinstated_at,
            timestamp_millis(effective_at) as effective_from,
            lead(timestamp_millis(effective_at), 1, timestamp("2999-01-01 00:00:00+00")) over (
                partition by policy_id order by effective_at
            ) as effective_to,
            max(version_id) over (partition by policy_id order by version_id desc) as latest_version
        from extended_ledger
    )
select *
from policy_ledger
