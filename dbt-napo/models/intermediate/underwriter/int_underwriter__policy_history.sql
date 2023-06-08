{{ config(
    pre_hook=[
      "{{declare_underwriter_udfs()}}"
    ]
) }}

SELECT
  customer.postal_code AS customer_postal_code,
  {{target.schema}}.calculate_postcode_area(customer.postal_code) AS customer_postcode_area,
  postcode_area_region.region AS customer_region,
  policy.reference_number AS policy_reference_number,
  policy.uuid AS policy_uuid,
  policy.policy_id as policy_id,
  product.reference AS product_reference,
  policy.annual_price AS policy_annual_retail_price,
  policy.annual_price AS policy_annual_premium_price,
  {{target.schema}}.calculate_premium_price(policy.annual_price, discount.discount_percentage) as policy_annual_premium_price_inc_discount,  
  customer.date_of_birth AS customer_date_of_birth,
  policy.created_date AS policy_created_date,
  policy.start_date AS policy_start_date,
  policy.end_date AS policy_end_date,
  policy.illness_cover_start_date AS policy_illness_cover_start_date,
  policy.accident_cover_start_date AS policy_accident_cover_start_date,
  policy.cancel_date AS policy_cancel_date,
  policy.cancel_date IS NOT NULL
  AND (
    policy.annual_payment_id IS NOT NULL
    OR subscription.policy_id IS NOT NULL
  ) AS policy_is_cancelled,
  renewal.old_policy_id IS NOT NULL AS policy_is_renewal,
  (
    policy.annual_payment_id IS NOT NULL
    OR subscription.policy_id IS NOT NULL
  ) AS policy_is_purchased,
  {{target.schema}}.calculate_policy_has_co_pay({{target.schema}}.calculate_age_in_months(pet.date_of_birth, policy.start_date)) AS policy_has_co_pay,
  CASE
    WHEN {{target.schema}}.calculate_policy_has_co_pay(
      {{target.schema}}.calculate_age_in_months(pet.date_of_birth, policy.start_date)
    ) IS NULL 
    THEN NULL
    WHEN {{target.schema}}.calculate_policy_has_co_pay(
      {{target.schema}}.calculate_age_in_months(pet.date_of_birth, policy.start_date)
    ) IS TRUE 
    THEN product.co_pay
    ELSE 0
  END AS policy_co_pay_percent,
  policy.payment_plan_type AS policy_payment_plan_type,
  {{target.schema}}.calculate_gross_written_premium(policy.annual_price, policy.start_date, policy.cancel_date) AS policy_gross_written_premium,
  EXTRACT(YEAR FROM policy.start_date) AS policy_start_date_year,
  renewal.old_policy_id AS renewal_old_policy_id,
  product.vet_fee_cover AS product_vet_fee_cover,
  product.complementary_treatment_cover AS product_complementary_treatment_cover,
  product.dental_cover AS product_dental_cover,
  product.emergency_boarding_cover AS product_emergency_boarding_cover,
  product.third_person_liability_cover AS product_third_person_liability_cover,
  product.pet_death_cover AS product_pet_death_cover,
  product.travel_cover AS product_travel_cover,
  product.missing_pet_cover AS product_missing_pet_cover,
  product.behavioural_treatment_cover AS product_behavioural_treatment_cover,
  product.excess AS product_excess,
  product.co_pay AS product_co_pay,
  pet.date_of_birth AS pet_date_of_birth,
  {{target.schema}}.calculate_age_in_years(pet.date_of_birth, policy.start_date) AS pet_age_in_years_at_start_date,
  {{target.schema}}.calculate_age_in_months(pet.date_of_birth, policy.start_date) AS pet_age_in_months_at_start_date,
  pet.breed_name AS pet_source_breed_name,
  pet.species AS pet_species,
  pet.gender AS pet_gender_iso_5218,
  pet.is_neutered AS pet_is_neutered,
  pet.is_microchipped AS pet_is_microchipped,
  pet.size AS pet_size,
  pet.breed_category AS pet_breed_category,
  quote.pricing_model_version AS quote_pricing_model_version,
  row_effective_from AS effective_from,
  row_effective_to AS effective_to
FROM
  {{ ref('int_policy_history') }}
  LEFT JOIN (
    SELECT
      policy_id,
      MIN(created_date) AS created_date
    FROM
      raw.subscription
    GROUP BY
      policy_id
  ) AS subscription ON policy.policy_id = subscription.policy_id
  AND TIMESTAMP_MILLIS(subscription.created_date) <= policy.effective_to
  LEFT JOIN {{ref("stg_raw__product")}} AS product ON policy.product_id = product.id
  LEFT JOIN raw.renewal ON policy.policy_id = renewal.new_policy_id
  LEFT JOIN {{ ref('postcode_area_region') }} ON postcode_area_region.postcode_area = {{target.schema}}.calculate_postcode_area(customer.postal_code)
WHERE (
  policy.annual_payment_id IS NOT NULL
  OR subscription.policy_id IS NOT NULL
)