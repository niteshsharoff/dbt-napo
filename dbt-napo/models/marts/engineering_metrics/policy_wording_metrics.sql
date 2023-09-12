{{ config(schema="marts") }}
{{ config(pre_hook=["{{declare_policy_udfs()}}"]) }}

select p.wording_document, p.reference_number, p.created_date, p.start_date
from {{ ref("stg_raw__policy") }} p
where
    {{ target.schema }}.is_sold(p.annual_payment_id, p.subscription_id) = true
    and p.cancel_date is null
