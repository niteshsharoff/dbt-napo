{% macro declare_underwriter_udfs () %}
CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_policy_has_co_pay (pet_age_months INT64) RETURNS BOOL AS (pet_age_months > (9 * 12));


CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_claim_excess_amount (
    claim_is_continuation BOOL,
    product_excess FLOAT64,
    claim_invoice_amount FLOAT64
  ) RETURNS FLOAT64 AS (
    CASE
      WHEN claim_is_continuation IS NULL THEN NULL
      WHEN claim_is_continuation = TRUE THEN 0
      WHEN claim_is_continuation = FALSE THEN LEAST(product_excess, claim_invoice_amount)
    END
  );


CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_claim_reserve_amount (
    claim_invoice_amount FLOAT64,
    claim_status STRING,
    claim_is_continuation BOOL,
    product_excess FLOAT64,
    claim_co_pay_percent INT64
  ) RETURNS FLOAT64 AS (
    CASE
      WHEN claim_invoice_amount IS NULL THEN 1
      WHEN claim_status IN ('declined', 'accepted', 'abeyance') THEN 0
      ELSE (
        claim_invoice_amount - {{target.schema}}.calculate_claim_excess_amount (
          claim_is_continuation,
          product_excess,
          claim_invoice_amount
        )
      ) * ((100 - claim_co_pay_percent) / 100)
    END
  );


create or replace function
  {{target.schema}}.calculate_premium_price (
    retail_price float64,
    discount_percentage numeric
  ) returns float64 as (
    case
      when discount_percentage is not null
      then round(retail_price / (1 - discount_percentage / 100), 2)
      else retail_price
    end
  );


{% endmacro %}
