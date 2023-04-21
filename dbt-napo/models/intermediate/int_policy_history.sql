WITH
  policy AS (
    SELECT
      * EXCEPT (
        start_date,
        end_date,
        created_date,
        cancel_date,
        accident_cover_start_date,
        illness_cover_start_date,
        change_at,
        effective_at
      ),
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(start_date)
      ) AS start_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(end_date)
      ) AS end_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(created_date)
      ) AS created_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(cancel_date)
      ) AS cancel_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(accident_cover_start_date)
      ) AS accident_cover_start_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(illness_cover_start_date)
      ) AS illness_cover_start_date,
      TIMESTAMP_MILLIS(change_at) AS change_at,
      TIMESTAMP_MILLIS(effective_at) AS effective_from,
      COALESCE(
        LEAD(TIMESTAMP_MILLIS(effective_at)) OVER (
          PARTITION BY
            policy_id
          ORDER BY
            effective_at
        ),
        TIMESTAMP("2999-01-01 00:00:00+00")
      ) AS effective_to
    FROM
      raw.policy
  ),
  customer AS (
    SELECT
      * EXCEPT (
        created_date,
        date_of_birth,
        change_at,
        effective_at
      ),
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(created_date)
      ) AS created_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(date_of_birth)
      ) AS date_of_birth,
      TIMESTAMP_MILLIS(change_at) AS change_at,
      TIMESTAMP_MILLIS(effective_at) AS effective_from,
      COALESCE(
        LEAD(TIMESTAMP_MILLIS(effective_at)) OVER (
          PARTITION BY
            customer_id
          ORDER BY
            effective_at
        ),
        TIMESTAMP("2999-01-01 00:00:00+00")
      ) AS effective_to
    FROM
      raw.customer
  ), pet AS (
    SELECT
      * EXCEPT (created_date, date_of_birth, change_at, effective_at),
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(created_date)
      ) AS created_date,
      EXTRACT(
        DATE
        FROM
          TIMESTAMP_MILLIS(date_of_birth)
      ) AS date_of_birth,
      TIMESTAMP_MILLIS(change_at) AS change_at,
      TIMESTAMP_MILLIS(effective_at) AS effective_from,
      COALESCE(
        LEAD(TIMESTAMP_MILLIS(effective_at)) OVER (
          PARTITION BY
            pet_id
          ORDER BY
            effective_at
        ),
        TIMESTAMP("2999-01-01 00:00:00+00")
      ) AS effective_to
    FROM
      raw.pet
  )
SELECT
  policy,
  customer,
  pet,
  GREATEST(pet.effective_from, row_effective_from) AS row_effective_from,
  LEAST(pet.effective_to, row_effective_to) AS row_effective_to
FROM
  (
    SELECT
      policy,
      customer,
      GREATEST(customer.effective_from, policy.effective_from) AS row_effective_from,
      LEAST(customer.effective_to, policy.effective_to) AS row_effective_to
    FROM
      policy
      LEFT JOIN customer ON policy.customer_id = customer.customer_id
      AND customer.effective_to >= policy.effective_from
      AND customer.effective_from < policy.effective_to
  )
  LEFT JOIN pet ON policy.pet_id = pet.pet_id
  AND pet.effective_to >= row_effective_from
  AND pet.effective_from < row_effective_to
