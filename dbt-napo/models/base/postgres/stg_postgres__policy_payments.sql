select pk as payment_id, fields.* from {{ source("postgres", "policy_payment") }}
