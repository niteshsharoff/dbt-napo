with
    policy as (
        select * 
        from {{ ref("stg_raw__policy_ledger") }}
    ),
    customer as (
        select * 
        from {{ ref("stg_raw__customer_ledger") }}
    ),
    pet as (
        select * 
        from {{ ref("stg_raw__pet_ledger") }}
    ),
    user as (
        select * 
        from {{ source("raw", "user") }}
    ),
    product as (
        select * 
        from {{ source("raw", "product") }}
    ),
    breed as (
        select *
        from {{ source("raw", "breed") }}
        where run_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
    ),
    quote as (
        select 
            quote_request_id, 
            msm_sales_tracking_urn, 
            created_at
        from {{ source("raw", "quoterequest") }}
    ),
    discount as (select * from {{ ref("stg_raw__vouchercode") }}),
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
        where policy.reference_number = 'ESS-BIN-0011'
    ),
    joint_history_with_change_audit as (
        select
            *,
            struct(
                {% for tuple in [
                    ["policy", "annual_price"], 
                    ["policy", "pet_id"],
                    ["policy", "customer_id"],
                    ["policy", "start_date"],
                    ["policy", "end_date"],
                    ["policy", "cancel_date"],
                    ["customer", "postal_code"],
                    ["pet", "name"],
                    ["pet", "breed_category"],
                    ["pet", "date_of_birth"],
                ] -%}
                {% set model = tuple[0] -%}
                {% set column = tuple[1] -%}
                case
                    when {{ model }}.{{ column }} != lag({{ model }}.{{ column }}) over (partition by policy.policy_id order by row_effective_from)
                    or (
                        {{ model }}.{{ column }} is not null 
                        and lag({{ model }}.{{ column }}) over (partition by policy.policy_id order by row_effective_from) is null
                    )
                    then true
                    else false
                end as {{ model }}_{{ column }}_changed
                {%- if not loop.last %}, {% endif %}
                {% endfor %}
            ) as mta
        from joint_history
    )
select
    quote,
    policy,
    product,
    customer,
    user,
    pet,
    breed,
    discount,
    mta,
    row_effective_from,
    row_effective_to,
from joint_history_with_change_audit j
left join user on j.customer.user_id = user.id
left join product on j.policy.product_id = product.id
left join breed on j.pet.breed_id = breed.id
left join quote on j.policy.quote_id = quote.quote_request_id
left join discount on j.policy.voucher_code_id = discount.voucher_id
