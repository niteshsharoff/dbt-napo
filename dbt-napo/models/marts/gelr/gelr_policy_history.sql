{{ config(schema="marts", materialized="table") }}

with
    dim_policy as (select * from {{ ref("dim_policy") }}),
    dim_customer as (select * from {{ ref("dim_customer") }}),
    dim_pet as (select * from {{ ref("dim_pet") }}),
    dim_quote as (select * from {{ ref("dim_quote") }}),
    policy_history as (
        select
            quote,
            policy,
            pet,
            customer,
            {{ target.schema }}.calculate_premium_price(
                policy.retail_price, quote.discount_percentage
            ) as premium_price,
            op.original_policy_id,
            ifnull(op.current_policy_year, 0) + 1 as policy_year,
            greatest(
                greatest(policy.effective_from, pet.effective_from),
                customer.effective_from
            ) as effective_from,
            least(
                least(policy.effective_to, pet.effective_to), customer.effective_to
            ) as effective_to
        from dim_policy policy
        join
            dim_customer customer
            on policy.customer_uuid = customer.customer_uuid
            and policy.effective_from <= customer.effective_to
            and policy.effective_to >= customer.effective_from
        join
            dim_pet pet
            on policy.pet_uuid = pet.pet_uuid
            and policy.effective_from <= pet.effective_to
            and policy.effective_to >= pet.effective_from
            and customer.effective_from <= pet.effective_to
            and customer.effective_to >= pet.effective_from
        left join dim_quote quote on policy.quote_uuid = quote.quote_uuid
        left join {{ ref("int_original_policy") }} op on policy.policy_id = op.policy_id
    )
select *
from policy_history
