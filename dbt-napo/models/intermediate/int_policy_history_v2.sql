{{ config(materialized="table") }}

{% set today = modules.datetime.datetime.now() %}
{% set yesterday = (today - modules.datetime.timedelta(1)).date() %}

with
    policy as (
        select
            policy.policy_id,
            policy.quote_id,
            policy.product_id,
            policy.customer_id,
            policy.pet_id,
            policy.voucher_code_id,
            original_policy.current_policy_year,
            original_policy.original_policy_id,
            original_policy.original_quote_source,
            policy.uuid,
            policy.reference_number,
            policy.quote_source,
            policy.quote_source_reference,
            policy.cancel_reason as cancel_reason_id,
            cancel_mapping.cancel_reason,
            policy.cancel_detail,
            policy.payment_plan_type,
            policy.annual_payment_id,
            policy.annual_price,
            policy.notes,
            policy.accident_cover_start_date,
            policy.illness_cover_start_date,
            policy.created_date,
            policy.start_date,
            policy.end_date,
            policy.cancel_date,
            policy.sold_at,
            policy.cancelled_at,
            policy.reinstated_at,
            policy.change_reason,
            policy.effective_from,
            policy.effective_to
        from {{ ref("stg_raw__policy_ledger") }} policy
        left join
            {{ ref("lookup_policy_cancel_reason") }} cancel_mapping
            on policy.cancel_reason = cancel_mapping.id
        left join
            {{ ref("int_original_policy") }} original_policy
            on policy.policy_id = original_policy.policy_id
    ),
    product as (
        select
            product.id,
            product.reference,
            product.name,
            product.vet_fee_cover,
            product.complementary_treatment_cover,
            product.dental_cover,
            product.emergency_boarding_cover,
            product.third_person_liability_excess,
            product.third_person_liability_cover,
            product.pet_death_cover,
            product.travel_cover,
            product.missing_pet_cover,
            product.behavioural_treatment_cover,
            product.co_pay,
            product.excess
        from {{ ref("stg_raw__product") }} product
    ),
    customer as (
        select
            customer.customer_id,
            customer.uuid,
            user.first_name,
            user.last_name,
            user.email,
            customer.street_address,
            customer.address_locality,
            customer.address_region,
            customer.postal_code,
            customer.date_of_birth,
            customer.change_reason,
            customer.effective_from,
            customer.effective_to
        from {{ ref("stg_raw__customer_ledger") }} customer
        left join {{ source("raw", "user") }} user on customer.user_id = user.id
    ),
    pet as (
        select
            pet.pet_id,
            pet.uuid,
            pet.name,
            pet.date_of_birth,
            pet.gender,
            pet.size,
            pet.cost,
            pet.is_neutered,
            pet.is_microchipped,
            pet.is_vaccinated,
            pet.species,
            pet.breed_category,
            breed.name as breed_name,
            breed.source as breed_source,
            pet.has_pre_existing_conditions,
            pet.change_reason,
            pet.multipet_number,
            pet.effective_from,
            pet.effective_to
        from {{ ref("stg_raw__pet_ledger") }} pet
        left join
            {{ source("raw", "breed") }} breed
            on pet.breed_id = breed.id
            and breed.run_date = parse_date('%Y-%m-%d', '{{yesterday}}')
    ),
    quote as (
        select
            quote.quote_id,
            quote.pricing_model_version,
            quote.msm_sales_tracking_urn,
            quote.created_at,
            quote.source,
            quote.discount_type
        from {{ ref("int_policy_quote") }} quote
    ),
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
left join
    campaign
    on (
        quote.quote_id = campaign.quote_id
        or j.policy.voucher_code_id = campaign.voucher_id
    )
    and coalesce(quote.discount_type, '')
    not in ('multipet', 'multipet_with_voucher_code')
