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
                mta.policy_annual_price_changed
                or mta.policy_start_date_changed
                or mta.policy_end_date_changed
                or mta.customer_postal_code_changed
                or mta.pet_name_changed
                or mta.pet_breed_category_changed
                or mta.pet_date_of_birth_changed
            )
        )
    ),
    renewals as (
        select 'Renewal' as transaction_type, row_effective_from as transaction_at, *
        from policy_history
        where row_effective_from = policy.sold_at and policy.quote_source = 'renewal'
    ),
    reinstatements as (
        select 'Reinstatement' as transaction_type, row_effective_from as transaction_at, *
        from policy_history
        where row_effective_from = policy.reinstated_at
    ),
    cancelled_reinstatements as (
        select 'Cancel Reinstatement' as transaction_type, row_effective_from as transaction_at, *
        from policy_history
        where row_effective_from = policy.cancelled_at and policy.reinstated_at is not null
    ),
    all_transactions as (
        select * from sold_policies
        union all select * from first_time_cancellations
        union all select * from mtas
        union all select * from renewals
        union all select * from reinstatements
        union all select * from cancelled_reinstatements
    )
select 
    transaction_type, 
    transaction_at, 
    quote, 
    policy, 
    product, 
    customer, 
    user, 
    pet, 
    breed,
    mta
from all_transactions t
order by transaction_at
