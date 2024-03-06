{{ config(materialized="view") }}

select *
from {{ source("airflow", "policy_claim_snapshot") }}
