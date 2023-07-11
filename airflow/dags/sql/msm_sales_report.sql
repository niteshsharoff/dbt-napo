with
    report as (
        select
            date('{{ start_date }}') as start_date,
            date('{{ end_date }}') as end_date,
            'moneysupermarket' as pcw_name
    ),
    new_policies as (
        select *
        from dbt_marts.reporting_policy_transaction, report
        where
            policy.quote_source = pcw_name
            and transaction_type = 'New Policy'
            and (
                cast(policy.sold_at as date) >= start_date
                and cast(policy.sold_at as date) < end_date
            )
    ),
    cancellations as (
        select *
        from dbt_marts.reporting_policy_transaction, report
        where
            policy.quote_source = pcw_name
            and transaction_type = 'NTU'
            and (
                cast(policy.cancelled_at as date) >= start_date
                and cast(policy.cancelled_at as date) < end_date
            )
    -- NTU
    -- and date_diff(policy.cancel_date, policy.start_date, day) < 14
    ),
    all_rows as (
        select *
        from new_policies
        union all
        select *
        from cancellations
    ),
    msm_report as (
        select
            case
                when transaction_type = 'NTU' then 'Cancellation' else 'Sale'
            end as recordtype,
            format_timestamp('%h-%Y', policy.created_date) as salesmonth,
            'Pet' as product,
            'Napo' as provider,
            'Napo' as brand,
            quote.msm_sales_tracking_urn as urn,
            customer.email as emailaddress,
            customer.street_address as housenameornumber,
            customer.postal_code as postcode,
            customer.first_name as firstname,
            customer.last_name as surname,
            format_timestamp('%d/%m/%Y', customer.date_of_birth) as dob,
            format_timestamp('%d/%m/%Y', policy.start_date) as policystartdate,
            quote.quote_id as providerquotereference,
            policy.reference_number as policyquotereference,
            'Online' as purchasechannel,
            product.reference as producttype,
            '1' as iptincluded,
            case
                when transaction_type = 'NTU'
                then cast(-1.0 * policy.annual_price as string)
                else cast(policy.annual_price as string)
            end as premium,
            format_timestamp('%d/%m/%Y', policy.sold_at) as policypurchasedate,
            c.cancel_reason as cancellationreason,
            format_timestamp('%d/%m/%Y', policy.cancel_date) as cancellationdate,
            case
                when policy.payment_plan_type = 'monthly'
                then 'Monthly'
                when policy.payment_plan_type = 'annually'
                then 'Annual'
                else ''
            end as purchasetype,
            '1' as originalquote,
            concat('MP', pet.multipet_number) as `Multi Pet`,
            initcap(pet.species) as animaltype,
            pet.name as `Pet Name`,
            'Life Time' as covertype
        from all_rows r
        left join dbt.lookup_msm_cancel_reason c on r.policy.cancel_reason_id = c.id
    )
select distinct *
from msm_report
