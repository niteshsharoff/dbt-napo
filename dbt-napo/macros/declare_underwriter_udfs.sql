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
    claim_co_pay_percent INT64,
    product_cover FLOAT64,
    policy_paid FLOAT64
  ) RETURNS FLOAT64 AS (
    CASE
      WHEN claim_invoice_amount IS NULL THEN 1
      WHEN claim_status IN ('declined', 'accepted', 'abeyance') THEN 0
      ELSE LEAST(
        (
          claim_invoice_amount - {{target.schema}}.calculate_claim_excess_amount (
            claim_is_continuation,
            product_excess,
            claim_invoice_amount
          )
        ) * ((100 - claim_co_pay_percent) / 100),
        product_cover - policy_paid
      )
    END
  );

CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_claim_incurred_amount (
    claim_reserve_amount FLOAT64,
    claim_paid_amount FLOAT64,
    claim_recovery_amount FLOAT64
  ) RETURNS FLOAT64 AS (
    CASE
      WHEN claim_paid_amount IS NULL THEN claim_reserve_amount - COALESCE(claim_recovery_amount, 0)
      ELSE claim_paid_amount - COALESCE(claim_recovery_amount, 0)
    END
  );

CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_policy_exposure(policy_start_date DATE, snapshot_date DATE) RETURNS FLOAT64 AS (
    LEAST(DATE_DIFF(GREATEST(snapshot_date, policy_start_date), policy_start_date, DAY), 366) / 366
  );

CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_policy_development_month(policy_start_date DATE, policy_end_date DATE, snapshot_date DATE) RETURNS FLOAT64 AS (
    CASE
      WHEN snapshot_date < policy_start_date THEN NULL
      ELSE DATE_DIFF(LEAST(snapshot_date, policy_end_date), policy_start_date, MONTH)
    END
  );

CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_postcode_area(postal_code STRING) RETURNS STRING AS (
    CASE
      WHEN REGEXP_CONTAINS(postal_code, r"^[A-Z][A-Z]") THEN SUBSTR(postal_code, 1, 2)
      WHEN REGEXP_CONTAINS(postal_code, r"^[A-Z][0-9]") THEN SUBSTR(postal_code, 1, 1)
    END
  );

CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_age_in_months(date_of_birth DATE, date_at DATE) RETURNS INT64 AS (
    CASE
      WHEN date_at < date_of_birth THEN 0
      WHEN EXTRACT(DAY FROM date_at) >= EXTRACT(DAY FROM date_of_birth) THEN DATE_DIFF(date_at, date_of_birth, MONTH)
      ELSE DATE_DIFF(date_at, date_of_birth, MONTH) - 1
    END
  );

CREATE OR REPLACE FUNCTION
  {{target.schema}}.calculate_age_in_years(date_of_birth DATE, date_at DATE) RETURNS INT64 AS (
    CAST(FLOOR({{target.schema}}.calculate_age_in_months(date_of_birth, date_at) / 12) AS INT64)
  );

CREATE OR REPLACE FUNCTION -- TODO: Include retail discounts
  {{target.schema}}.calculate_gross_written_premium(policy_annual_price FLOAT64, policy_start_date DATE, policy_cancel_date DATE) RETURNS FLOAT64 AS (
    CASE
      WHEN policy_cancel_date IS NULL THEN policy_annual_price / 1.12
      WHEN DATE_DIFF(policy_cancel_date, policy_start_date, DAY) <= 14 THEN 0
      ELSE (policy_annual_price / 1.12) * {{target.schema}}.calculate_policy_exposure(policy_start_date, policy_cancel_date)
    END
  );

CREATE OR REPLACE FUNCTION -- TODO: Include retail discounts
{{target.schema}}.calculate_gross_earned_premium(policy_annual_price FLOAT64, policy_start_date DATE, policy_cancel_date DATE, snapshot_date DATE) RETURNS FLOAT64 AS (
    CASE
      WHEN policy_cancel_date IS NULL THEN (policy_annual_price / 1.12) * {{target.schema}}.calculate_policy_exposure(policy_start_date, snapshot_date)
      WHEN DATE_DIFF(policy_cancel_date, policy_start_date, DAY) <= 14 THEN 0
      ELSE (policy_annual_price / 1.12) * {{target.schema}}.calculate_policy_exposure(policy_start_date, LEAST(snapshot_date, policy_cancel_date))
    END
);

create or replace function
  {{target.schema}}.calculate_premium_price (
    retail_price float64,
    discount_percentage float64
  ) returns float64 as (
    case
      when discount_percentage is not null
      then retail_price / (1 - discount_percentage / 100)
      else retail_price
    end
  );

  
create or replace function
  {{target.schema}}.calculate_retail_price (
    premium_price float64,
    discount_percentage float64
  ) returns float64 as (
    case
      when discount_percentage is not null
      then premium_price * (1 - discount_percentage / 100)
      else premium_price
    end
  );


create or replace function
  {{target.schema}}.calculate_consumed_amount(
    amount float64,
    start_date date, 
    end_date date,
    cancel_date date
  ) returns float64 as (
    case
      when date_diff(end_date, start_date, day) > 0
      then greatest(amount * (date_diff(cancel_date, start_date, day) / date_diff(end_date, start_date, day)), 0)
      else 0.0
    end
  );


create or replace function
  {{target.schema}}.calculate_amount_exc_ipt(amount float64) returns float64 as (
    amount / (1 + 12 / 100)
  );


{% endmacro %}
