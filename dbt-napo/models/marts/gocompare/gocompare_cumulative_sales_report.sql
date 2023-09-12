{{ config(schema="marts") }}

with
    new_policies as (
        select *
        from {{ ref("reporting_policy_transaction") }}
        where policy.quote_source = 'gocompare' and transaction_type = 'New Policy'
    ),
    all_ntus as (
        select
            *,
            transaction_at = min(transaction_at) over (
                partition by policy.reference_number
            ) as _include
        from {{ ref("reporting_policy_transaction") }}
        where policy.quote_source = 'gocompare' and transaction_type = 'NTU'
    ),
    new_ntus as (select * except (_include) from all_ntus where _include = true),
    all_rows as (
        select *
        from new_policies
        union all
        select *
        from new_ntus
    ),
    report as (
        select
            -- for calculating reporting period, transaction timestamp is excluded
            -- from final report
            transaction_at as _transaction_at,
            91 as providerid,
            'Napo' as providername,
            lower(product.name) as policyname,
            trim(
                substring(customer.postal_code, 1, length(customer.postal_code) - 3)
            ) as postcode,
            cast(policy.sold_at as date) as purchasedate,
            policy.start_date as policystartdate,
            format('%.2f', policy.annual_price) as pricepaid,
            'Online' as purchasemethod,
            policy.quote_source_reference as quoterequestid,
            count(distinct policy.reference_number) over (
                partition by quote.quote_id
            ) as numberofpets,
            case
                when
                    count(distinct policy.reference_number) over (
                        partition by quote.quote_id
                    )
                    > 1
                then 1
                else 0
            end as multipetsale,
            initcap(pet.species) as species,
            case
                when pet.breed_name is null and pet.species = 'dog'
                then 'Mongrel'
                when pet.breed_name is null and pet.species = 'cat'
                then 'Moggie'
                else pet.breed_name
            end as breed,
            policy.cancel_date as cancellationdate,
            case
                when
                    policy.cancel_reason in (
                        select cancel_reason
                        from {{ ref("lookup_gocompare_cancel_reason") }}
                    )
                then 1
                when transaction_type = 'NTU'
                then 0
                else null
            end as customercancelled,
            case
                when transaction_type = 'NTU' and policy.cancel_reason = 'fraud'
                then 1
                when transaction_type = 'NTU'
                then 0
                else null
            end as fraud,
            null as fraudreason,
            policy.reference_number as policyno,
            'GoCompare' as pcw
        from all_rows
    )
select distinct *
from report
order by cancellationdate, purchasedate
