{{ config(schema="marts") }}
{{ config(pre_hook=["{{declare_policy_udfs()}}"]) }}

select p.policy_id, p.start_date, p.end_date, r.old_policy_id as renewal_old_policy_id
from {{ ref("stg_raw__policy") }} p
left join {{ ref("stg_raw__renewal") }} r on r.old_policy_id = p.policy_id
where
    {{ target.schema }}.is_sold(p.annual_payment_id, p.subscription_id) = true
    and p.cancel_date is null
    and p.end_date between current_date() and date_add(current_date(), interval 28 day)
