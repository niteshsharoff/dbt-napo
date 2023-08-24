{{ config(schema="marts") }}

with
    new_policies as (
        select *
        from {{ ref("reporting_policy_transaction") }}
        where
            policy.quote_source = 'moneysupermarket' and transaction_type = 'New Policy'
    ),
    all_ntus as (
        select
            *,
            transaction_at = min(transaction_at) over (
                partition by policy.reference_number
            ) as _include
        from {{ ref("reporting_policy_transaction") }}
        where policy.quote_source = 'moneysupermarket' and transaction_type = 'NTU'
    ),
    new_ntus as (select * except (_include) from all_ntus where _include = true),
    all_rows as (
        select *
        from new_policies
        union all
        select *
        from new_ntus
    ),
    msm_report as (
        select
            transaction_at as _transaction_at,
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
        left join
            {{ ref("lookup_msm_cancel_reason") }} c on r.policy.cancel_reason_id = c.id
    )
select distinct *
from msm_report
order by cancellationdate, _transaction_at
