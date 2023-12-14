{{ config(schema="marts") }}

select *
from {{ source("airflow", "policy_claim_snapshot") }}
