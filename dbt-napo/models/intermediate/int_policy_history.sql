{% set today = modules.datetime.datetime.now() %}
{% set yesterday = (today - modules.datetime.timedelta(1)).date() %}

with
    policy as (
        select
            * except (policy_id, cancel_reason, run_date),
            policy.policy_id,
            policy.cancel_reason as cancel_reason_id,
            cancel_mapping.cancel_reason
        from {{ ref("stg_raw__policy_ledger") }} policy
        left join
            {{ ref("lookup_policy_cancel_reason") }} cancel_mapping
            on policy.cancel_reason = cancel_mapping.id
        left join
            {{ ref("int_original_policy") }} original_policy
            on policy.policy_id = original_policy.policy_id
    ),
    product as (select * from {{ source("raw", "product") }}),
    customer as (
        select * except (run_date), customer.run_date
        from {{ ref("stg_raw__customer_ledger") }} customer
        left join {{ source("raw", "user") }} user on customer.user_id = user.id
    ),
    pet as (
        select
            * except (run_date, name, species, source),
            pet.run_date,
            pet.name,
            pet.species,
            breed.name as breed_name,
            breed.source as breed_source
        from {{ ref("stg_raw__pet_ledger") }} pet
        left join
            {{ source("raw", "breed") }} breed
            on pet.breed_id = breed.id
            and breed.run_date = parse_date('%Y-%m-%d', '{{yesterday}}')
    ),
    quote as (select * from {{ ref("int_policy_quote") }}),
    campaign as (select * from {{ ref("stg_raw__vouchercode") }}),
    joint_history as (
        select
            policy,
            customer,
            pet,
            greatest(pet.effective_from, row_effective_from) as row_effective_from,
            least(pet.effective_to, row_effective_to) as row_effective_to
        from
            (
                select
                    policy,
                    customer,
                    greatest(
                        customer.effective_from, policy.effective_from
                    ) as row_effective_from,
                    least(
                        customer.effective_to, policy.effective_to
                    ) as row_effective_to
                from policy
                left join
                    customer
                    on policy.customer_id = customer.customer_id
                    and customer.effective_to >= policy.effective_from
                    and customer.effective_from < policy.effective_to
            )
        left join
            pet
            on policy.pet_id = pet.pet_id
            and pet.effective_to >= row_effective_from
            and pet.effective_from < row_effective_to
    )
select
    quote,
    policy,
    customer,
    pet,
    product,
    campaign,
    row_effective_from,
    row_effective_to,
from joint_history j
left join product on j.policy.product_id = product.id
left join quote on j.policy.quote_id = quote.quote_id
left join campaign on j.policy.voucher_code_id = campaign.voucher_id
