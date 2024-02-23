{{ config(materialized="view", schema="marts") }}

select *
from {{ source("airflow", "policy_claim_snapshot") }}
