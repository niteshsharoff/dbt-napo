{% set mta_fields = [
    ["policy", "annual_price"],
    ["policy", "accident_cover_start_date"],
    ["policy", "illness_cover_start_date"],
    ["policy", "start_date"],
    ["policy", "end_date"],
    ["policy", "cancel_date"],
    ["customer", "first_name"],
    ["customer", "last_name"],
    ["customer", "email"],
    ["customer", "date_of_birth"],
    ["customer", "postal_code"],
    ["pet", "name"],
    ["pet", "date_of_birth"],
    ["pet", "gender"],
    ["pet", "size"],
    ["pet", "cost"],
    ["pet", "is_neutered"],
    ["pet", "is_microchipped"],
    ["pet", "is_vaccinated"],
    ["pet", "species"],
    ["pet", "breed_category"],
    ["pet", "breed_name"],
    ["pet", "breed_source"],
    ["pet", "has_pre_existing_conditions"]
] %}

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
                {% for mta_field in mta_fields -%}
                {% set model = mta_field[0] -%}
                {% set column = mta_field[1] -%}
                _audit.{{ model }}_{{ column }}_changed
                {%- if not loop.last %} or{% endif %}
                {% endfor %}
            )
        )
    ),
    renewals as (
        select 'Renewal' as transaction_type, row_effective_from as transaction_at, *
        from policy_history r
        where policy.quote_source = 'renewal'
        and row_effective_from = (
            select min(policy.sold_at)
            from policy_history
            where policy.policy_id = r.policy.policy_id 
        )
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
    ),
    final as (
        select 
            transaction_type, 
            transaction_at,
            quote,
            (select as struct policy.* except(quote_id, product_id, customer_id, pet_id, voucher_id)) as policy, 
            customer, 
            pet,
            product,
            discount
        from all_transactions
        order by transaction_at
    )
select *
from final

