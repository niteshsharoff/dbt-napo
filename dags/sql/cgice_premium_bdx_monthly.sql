with report as (
  select date('{{ start_date }}') as t1
  , date('{{ end_date }}') as t2
)
, drop_reinstated_cancellations as (
    -- This is a requirement from CGICE, reinstatements and cancellations adding up to
    -- a premium position of 0 should be removed as they cancel each other out
  select *
    , case
      when (
        transaction_type = 'Reinstatement'
        and lag(transaction_type) over(partition by policy_number order by transaction_at) = 'Cancel'
        and gross_premium_ipt_inc + lag(gross_premium_ipt_inc) over(partition by policy_number order by transaction_at) = 0
      )
      or (
        transaction_type = 'Cancel'
        and lead(transaction_type) over(partition by policy_number order by transaction_at) = 'Reinstatement'
        and gross_premium_ipt_inc + lead(gross_premium_ipt_inc) over(partition by policy_number order by transaction_at) = 0
      )
      then True
      else False
    end as _exclude
  from dbt_jeremiahmai.cumulative_premium_bdx, report
  where transaction_date >= report.t1 and transaction_date < report.t2
)
, monthly_premium_bdx as (
  select * except(_exclude)
  from drop_reinstated_cancellations
  where _exclude = False
)
select policy_number as `Policy Number`
  , quote_id as `Quote Reference`
  , transaction_type as `Status`
  , original_quote_source as `Original Source`
  , quote_source as `Current Source`
  , discount_amount as `Discount Campaign`
  , customer_name as `Insured Name`
  , customer_dob as `Insured DOB`
  , customer_postal_code as `Postcode`
  , product_reference as `Policy Type`
  , pet_id as `Unique Pet ID`
  , pet_species as `Pet Type`
  , pet_name as `Pet Name`
  , pet_breed as `Pet Breed`
  , pet_age as `Pet Age`
  , pet_gender as `Pet Gender`
  , pet_cost as `Pet Cost`
  , pet_chipped as `Pet Chipped`
  , pet_neutered as `Pet Neutered`
  , multipet as `Multipet`
  , transaction_date as `Transaction Date`
  , effective_date as `Effective Date`
  , original_issue_date as `Original Issue Date`
  , start_date as `Start Date`
  , end_date as `End Date`
  , policy_year as `Policy Year`
  , cancellation_date as `Cancellation Date`
  , cancellation_reason as `Cancellation Reason`
  , payment_method as `Payment Method`
  , payment_period as `Payment Period`
  , format('%.2f', gross_premium_ipt_exc) as `Gross Premium IPT exc`
  , format('%.2f', gross_premium_ipt_inc) as `Gross Premium IPT inc`
  , '12' as `IPT %`
  , format('%.2f', ipt) as `IPT`
  , 'new annual' as `Commission Type`
  , '35%' as `Commission Rate`
  , format('%.2f', annual_commission) as `Napo Annual Commission`
  , format('%.2f', net_rated_premium_due_to_underwriter) as `Net Rated Premium Due To Underwriter IPT exc`
  , format('%.2f', total_due_to_underwriter) as `Total Due To Underwriter IPT inc`
  , customer_id as `Primary Customer Id`
  , policy_id as `Policy Id`
from monthly_premium_bdx
order by original_issue_date, policy_id
