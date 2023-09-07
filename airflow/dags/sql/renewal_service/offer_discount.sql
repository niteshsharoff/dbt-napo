select
    renewal_date,
    renewal_uuid,
    case
        when eligible_discount_percent = 15
        then 'renewal_15'
        when eligible_discount_percent = 10
        then 'renewal_10'
        when eligible_discount_percent = 5
        then 'renewal_5'
    end as campaign_code
from `dbt_marts.promotion__discount_eligible_renewals`
where
    renewal_date <= '{{ renewal_date }}'
    and year_1_has_claims = false
    and customer_n_active_policies = 1
    and opted_out = true
    and eligible_discount_percent is not null
    and year_2_voucher_code is null
order by renewal_date
