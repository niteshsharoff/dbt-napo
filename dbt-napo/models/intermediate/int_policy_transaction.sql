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
    mtas as (
        select 'MTA' as transaction_type, row_effective_from as transaction_at, *
        from policy_history
        where (
            row_effective_from != policy.sold_at
            and (row_effective_from != policy.cancelled_at or policy.cancelled_at is null)
            and (
                _mta.policy_annual_price_changed
                or _mta.policy_start_date_changed
                or _mta.policy_end_date_changed
                or _mta.customer_postal_code_changed
                or _mta.pet_name_changed
                or _mta.pet_breed_category_changed
                or _mta.pet_date_of_birth_changed
            )
        )
    ),
    all_transactions as (
        select * from sold_policies
        union all
        select * from first_time_cancellations
    )
select transaction_type, transaction_at, quote, policy, product, customer, user, pet, breed
from all_transactions t
