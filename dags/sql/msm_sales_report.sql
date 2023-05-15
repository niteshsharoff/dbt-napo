with report as (
  select date('{{ start_date }}') as start_date
  , date('{{ end_date }}') as end_date
  , 'moneysupermarket' as pcw_name
)
, new_policies as (
  select *
  from dbt.int_policy_transaction, report
  where policy.quote_source = pcw_name
    and transaction_type = 'New Policy'
    and (cast(policy.sold_at as date) >= start_date and cast(policy.sold_at as date) < end_date)
)
, cancellations as (
  select *
  from dbt.int_policy_transaction, report
  where policy.quote_source = pcw_name
    and transaction_type = 'Cancel Policy'
    and (cast(policy.cancelled_at as date) >= start_date and cast(policy.cancelled_at as date) < end_date)
    -- NTU
    -- and date_diff(policy.cancel_date, policy.start_date, day) < 14
)
, all_rows as (
  select * from new_policies
  union all
  select * from cancellations
)
, msm_report as (
  select
    case
      when transaction_type = 'Cancel Policy' then 'Cancellation'
      else 'Sale'
    end as RecordType
    , format_timestamp('%h-%Y', policy.created_date) as SalesMonth
    , 'Pet' as Product
    , 'Napo' as Provider
    , 'Napo' as Brand
    , quote.msm_sales_tracking_urn as URN
    , user.email as EmailAddress
    , customer.street_address as HouseNameorNumber
    , customer.postal_code as Postcode
    , user.first_name as Firstname
    , user.last_name as Surname
    , format_timestamp('%d/%m/%Y', customer.date_of_birth) as DOB
    , format_timestamp('%d/%m/%Y', policy.start_date) as PolicyStartDate
    , policy.quote_id as ProviderQuoteReference
    , policy.reference_number as PolicyQuoteReference
    , 'Online' as PurchaseChannel
    , product.reference as ProductType
    , '1' as IPTIncluded
    , case
      when transaction_type = 'Cancel Policy' then -1.0 * policy.annual_price
      else policy.annual_price
    end as Premium
    , format_timestamp('%d/%m/%Y', policy.sold_at) as PolicyPurchaseDate
    , c.cancel_reason as CancellationReason
    , format_timestamp('%d/%m/%Y', policy.cancel_date) as CancellationDate
    , case
      when policy.payment_plan_type = 'monthly' then 'Monthly'
      when policy.payment_plan_type = 'annually' then 'Annual'
      else ''
    end as PurchaseType
    , '1' as OriginalQuote
    , concat('MP', pet.multipet_number) as `Multi Pet`
    , initcap(pet.species) as AnimalType
    , pet.name as `Pet Name`
    , 'Life Time' as CoverType
  from all_rows r
  left join dbt.lookup_msm_cancel_reason c on r.policy.cancel_reason = c.id
)
select distinct *
from msm_report