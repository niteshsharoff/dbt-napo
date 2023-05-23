{{ config(
    pre_hook=[
      "{{declare_underwriter_udfs()}}"
    ]
) }}

SELECT
  policy.reference_number AS policy_reference_number,
  policy.uuid AS policy_uuid,
  policy.policy_id as policy_id,
  product.reference AS product_reference,
  policy.annual_price AS policy_annual_retail_price,
  {{target.schema}}.calculate_premium_price(policy.annual_price, discount.discount_percentage) as policy_annual_premium_price,  
  customer.date_of_birth AS customer_date_of_birth,
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
  {{target.schema}}.calculate_policy_has_co_pay(pet.age_months) AS policy_has_co_pay,
  CASE
    WHEN {{target.schema}}.calculate_policy_has_co_pay(pet.age_months) IS NULL THEN NULL
    WHEN {{target.schema}}.calculate_policy_has_co_pay(pet.age_months) IS TRUE THEN product.co_pay
    ELSE 0
  END AS policy_co_pay_percent,
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
  pet.age_months AS pet_age_months,
  pet.date_of_birth AS pet_date_of_birth,
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
  AND TIMESTAMP_MILLIS(subscription.created_date) <= policy.effective_from
  LEFT JOIN raw.product ON policy.product_id = product.id
  LEFT JOIN raw.renewal ON policy.policy_id = renewal.new_policy_id
