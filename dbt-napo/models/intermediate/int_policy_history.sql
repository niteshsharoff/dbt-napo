with
    policy as (
        select policy_id
            , quote_id
            , product_id
            , customer_id
            , pet_id
            , voucher_code_id as voucher_id
            , uuid
            , reference_number
            , quote_source
            , payment_plan_type
            , annual_price as annual_retail_price
            , notes
            , accident_cover_start_date
            , illness_cover_start_date
            , start_date
            , end_date
            , cancel_date
            , cancel_detail
            , cancel_reason
            , sold_at
            , cancelled_at
            , reinstated_at
            , change_reason
            , effective_from
            , effective_to
        from {{ ref("stg_raw__policy_ledger") }}
    ),
    product as (
        select id as product_id
            , reference
            , name
            , vet_fee_cover
            , complementary_treatment_cover
            , dental_cover
            , emergency_boarding_cover
            , third_person_liability_excess
            , third_person_liability_cover
            , pet_death_cover
            , travel_cover
            , missing_pet_cover
            , behavioural_treatment_cover
            , co_pay
            , excess
        from {{ source("raw", "product") }}
    ),
    customer as (
        select customer.customer_id
            , customer.uuid
            , user.first_name
            , user.last_name
            , user.email
            , customer.street_address
            , customer.address_locality
            , customer.address_region
            , customer.postal_code
            , customer.date_of_birth
            , customer.change_reason
            , customer.effective_from
            , customer.effective_to
        from {{ ref("stg_raw__customer_ledger") }} customer
        left join {{ source("raw", "user") }} user on customer.user_id = user.id
    ),
    pet as (
        select pet.pet_id
            , pet.uuid
            , pet.name
            , pet.date_of_birth
            , case 
                when pet.gender = '1' then 'male'
                when pet.gender = '2' then 'female'
                else null
            end as gender
            , pet.size
            , pet.cost
            , pet.is_neutered
            , pet.is_microchipped
            , pet.is_vaccinated
            , pet.species
            , pet.breed_category
            , breed.name as breed_name
            , breed.source as breed_source
            , pet.has_pre_existing_conditions
            , pet.change_reason
            , pet.effective_from
            , pet.effective_to
        from {{ ref("stg_raw__pet_ledger") }} pet
        left join {{ source("raw", "breed") }} breed 
            on pet.breed_id = breed.id and breed.run_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
    ),
    quote as (
        select quote_request_id as quote_id
            , msm_sales_tracking_urn
            , timestamp_millis(created_at) as created_at
        from {{ source("raw", "quoterequest") }}
    ),
    discount as (
        select voucher_id
            , voucher_code
            , discount_percentage
            , affiliate_channel
        from {{ ref("stg_raw__vouchercode") }}
    ),
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
    ),
    joint_history_with_change_audit as (
        select
            *
            , struct(
                {% for mta_fields in [
                    ["policy", "annual_retail_price"],
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
                ] -%}
                {% set model = mta_fields[0] -%}
                {% set column = mta_fields[1] -%}
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
                , policy.change_reason as policy_row_change_reason
                , customer.change_reason as customer_row_change_reason
                , pet.change_reason as pet_row_change_reason
                , policy.effective_from as policy_row_effective_from
                , policy.effective_to as policy_row_effective_to
                , customer.effective_from as customer_row_effective_from
                , customer.effective_to as customer_row_effective_to
                , pet.effective_from as pet_row_effective_from
                , pet.effective_to as pet_row_effective_to
            ) as _audit
        from joint_history
    )
select
    quote,
    (select as struct policy.* except(change_reason, effective_from, effective_to)) as policy,
    (select as struct customer.* except(change_reason, effective_from, effective_to)) as customer,
    (select as struct pet.* except(change_reason, effective_from, effective_to)) as pet,
    product,
    discount,
    _audit,
    row_effective_from,
    row_effective_to,
from joint_history_with_change_audit j
left join product on j.policy.product_id = product.product_id
left join quote on j.policy.quote_id = quote.quote_id
left join discount on j.policy.voucher_id = discount.voucher_id
