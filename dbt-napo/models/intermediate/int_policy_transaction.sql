with
    policy_history as (select * from {{ ref("int_policy_history") }}),
    sold_policies as (
        select 'New Policy' as transaction_type, row_effective_from as transaction_at, *
        from policy_history
        where row_effective_from = policy.sold_at and policy.quote_source != 'renewal'
    ),
    first_time_cancellations as (
        select 'Cancel Policy' as transaction_type, row_effective_from as transaction_at, *
        from policy_history
        where row_effective_from = policy.cancelled_at and policy.reinstated_at is null
    ),
    all_transactions as (
        select * from sold_policies
        union all
        select * from first_time_cancellations
    )
select transaction_type, transaction_at, quote, policy, product, customer, user, pet, breed
from all_transactions t
