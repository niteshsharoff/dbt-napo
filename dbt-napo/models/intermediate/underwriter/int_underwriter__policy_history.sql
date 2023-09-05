{{ config(pre_hook=["{{declare_underwriter_udfs()}}"]) }}

select
    customer.postal_code as customer_postal_code,
    {{ target.schema }}.calculate_postcode_area(
        customer.postal_code
    ) as customer_postcode_area,
    postcode_area_region.region as customer_region,
    policy.reference_number as policy_reference_number,
    policy.uuid as policy_uuid,
    policy.policy_id as policy_id,
    product.reference as product_reference,
    policy.annual_price as policy_annual_retail_price,
    {{ target.schema }}.calculate_premium_price(
        policy.annual_price, campaign.discount_percentage
    ) as policy_annual_premium_price,
    customer.date_of_birth as customer_date_of_birth,
    policy.start_date as policy_start_date,
    policy.end_date as policy_end_date,
    policy.illness_cover_start_date as policy_illness_cover_start_date,
    policy.accident_cover_start_date as policy_accident_cover_start_date,
    policy.cancel_date as policy_cancel_date,
    policy.cancel_date is not null
    and (
        policy.annual_payment_id is not null or subscription.policy_id is not null
    ) as policy_is_cancelled,
    renewal.old_policy_id is not null as policy_is_renewal,
    (
        policy.annual_payment_id is not null or subscription.policy_id is not null
    ) as policy_is_purchased,
    {{ target.schema }}.calculate_policy_has_co_pay(
        {{ target.schema }}.calculate_age_in_months(
            pet.date_of_birth, policy.start_date
        )
    ) as policy_has_co_pay,
    case
        when
            {{ target.schema }}.calculate_policy_has_co_pay(
                {{ target.schema }}.calculate_age_in_months(
                    pet.date_of_birth, policy.start_date
                )
            )
            is null
        then null
        when
            {{ target.schema }}.calculate_policy_has_co_pay(
                {{ target.schema }}.calculate_age_in_months(
                    pet.date_of_birth, policy.start_date
                )
            )
            is true
        then product.co_pay
        else 0
    end as policy_co_pay_percent,
    policy.payment_plan_type as policy_payment_plan_type,
    {{ target.schema }}.calculate_gross_written_premium(
        {{ target.schema }}.calculate_premium_price(
            policy.annual_price, campaign.discount_percentage
        ),
        policy.start_date,
        policy.cancel_date
    ) as policy_gross_written_premium,
    extract(year from policy.start_date) as policy_start_date_year,
    renewal.old_policy_id as renewal_old_policy_id,
    product.vet_fee_cover as product_vet_fee_cover,
    product.complementary_treatment_cover as product_complementary_treatment_cover,
    product.dental_cover as product_dental_cover,
    product.emergency_boarding_cover as product_emergency_boarding_cover,
    product.third_person_liability_cover as product_third_person_liability_cover,
    product.pet_death_cover as product_pet_death_cover,
    product.travel_cover as product_travel_cover,
    product.missing_pet_cover as product_missing_pet_cover,
    product.behavioural_treatment_cover as product_behavioural_treatment_cover,
    product.excess as product_excess,
    product.co_pay as product_co_pay,
    pet.date_of_birth as pet_date_of_birth,
    {{ target.schema }}.calculate_age_in_years(
        pet.date_of_birth, policy.start_date
    ) as pet_age_in_years_at_start_date,
    {{ target.schema }}.calculate_age_in_months(
        pet.date_of_birth, policy.start_date
    ) as pet_age_in_months_at_start_date,
    pet.breed_name as pet_source_breed_name,
    pet.species as pet_species,
    pet.gender as pet_gender_iso_5218,
    pet.is_neutered as pet_is_neutered,
    pet.is_microchipped as pet_is_microchipped,
    pet.size as pet_size,
    pet.breed_category as pet_breed_category,
    quote.pricing_model_version as quote_pricing_model_version,
    quote.pricing_algorithm_version as quote_pricing_algorithm_version,
    row_effective_from as effective_from,
    row_effective_to as effective_to
from {{ ref("int_policy_history") }}
left join
    (
        select policy_id, min(created_date) as created_date
        from raw.subscription
        group by policy_id
    ) as subscription
    on policy.policy_id = subscription.policy_id
    and timestamp_millis(subscription.created_date) <= policy.effective_to
left join {{ ref("stg_raw__product") }} as product on policy.product_id = product.id
left join raw.renewal on policy.policy_id = renewal.new_policy_id
left join
    {{ ref("postcode_area_region") }}
    on postcode_area_region.postcode_area
    = {{ target.schema }}.calculate_postcode_area(customer.postal_code)
where (policy.annual_payment_id is not null or subscription.policy_id is not null)
