{{ config(schema="marts", pre_hook=["{{declare_policy_udfs()}}"]) }}

with
    policy_transactions as (select * from {{ ref("reporting_policy_transaction") }}),
    cgice_transactions as (
        select
            transaction_type,
            transaction_at,
            policy.reference_number as policy_number,
            quote.quote_id as quote_id,
            case
                when
                    policy.original_quote_source is null
                    and policy.quote_source != 'renewal'
                then policy.quote_source
                else policy.original_quote_source
            end as original_quote_source,
            policy.quote_source,
            finance.discount_difference as discount_amount,
            -- discounts information requested by Finance
            campaign.voucher_code as voucher_code,
            campaign.discount_percentage as discount_percentage,
            trim(customer.first_name)
            || ' '
            || trim(customer.last_name) as customer_name,
            customer.date_of_birth as customer_dob,
            customer.postal_code as customer_postal_code,
            product.reference as product_reference,
            pet.pet_id as pet_id,
            pet.species as pet_species,
            pet.name as pet_name,
            {{ target.schema }}.get_breed_for_pet(
                pet.species, pet.size, pet.breed_category, pet.breed_name
            ) as pet_breed,
            dbt_jeremiahmai.calculate_age_in_months(
                pet.date_of_birth, policy.start_date
            ) as pet_age,
            pet.gender as pet_gender,
            format('%.2f', pet.cost) as pet_cost,
            pet.is_microchipped as pet_chipped,
            pet.is_neutered as pet_neutered,
            case when pet.multipet_number is null then 'No' else 'Yes' end as multipet,
            cast(transaction_at as date) as transaction_date,
            case
                when transaction_type = 'Cancellation'
                then policy.cancel_date
                else cast(transaction_at as date)
            end as effective_date,
            policy.created_date as original_issue_date,
            policy.start_date as start_date,
            policy.end_date as end_date,
            policy.policy_year + 1 as policy_year,
            policy.cancel_date as cancellation_date,
            policy.cancel_reason as cancellation_reason,
            'Direct Debit' as payment_method,
            policy.payment_plan_type as payment_period,
            finance.premium_difference_ipt_inc
            - finance.ipt_difference as gross_premium_ipt_exc,
            finance.premium_difference_ipt_inc as gross_premium_ipt_inc,
            12 as ipt_percent,
            finance.ipt_difference as ipt,
            "new annual" as commission_type,
            "35%" as commission_rate,
            0.35 * finance.premium_difference_ipt_exc as annual_commission,
            0.65
            * finance.premium_difference_ipt_exc as net_rated_premium_due_to_underwriter
            ,
            0.65 * finance.premium_difference_ipt_exc
            + finance.ipt_difference as total_due_to_underwriter,
            customer.customer_id as customer_id,
            policy.policy_id as policy_id
        from policy_transactions
    ),
    unchanged_transactions as (
        select *
        from cgice_transactions
        where
            transaction_type = 'New Policy'
            or transaction_type = 'Renewal'
            or transaction_type = 'MTA'
            or transaction_type = 'Reinstatement'
    ),
    cancellations as (
        -- CGICE wants 'NTU', 'Cancellation' and 'Cancellation MTA' transactions to be
        -- called 'Cancel'
        select 'Cancel' as transaction_type, * except (transaction_type)
        from cgice_transactions
        where
            transaction_type = 'NTU'
            or transaction_type = 'Cancellation'
            or transaction_type = 'Cancellation MTA'
    ),
    all_transactions as (
        select *
        from unchanged_transactions
        union all
        select *
        from cancellations
    ),
    agg_differences_by_day as (
        -- CGICE wants multiple MTA or Cancel transactions to be aggregated by day
        select
            * except (
                discount_amount,
                gross_premium_ipt_exc,
                gross_premium_ipt_inc,
                ipt,
                annual_commission,
                net_rated_premium_due_to_underwriter,
                total_due_to_underwriter
            ),
            -- aggregate price change MTAs for discounts for Finance
            sum(discount_amount) over (
                partition by transaction_date, transaction_type, policy_number
            ) as discount_amount,
            sum(gross_premium_ipt_exc) over (
                partition by transaction_date, transaction_type, policy_number
            ) as gross_premium_ipt_exc,
            sum(gross_premium_ipt_inc) over (
                partition by transaction_date, transaction_type, policy_number
            ) as gross_premium_ipt_inc,
            sum(ipt) over (
                partition by transaction_date, transaction_type, policy_number
            ) as ipt,
            sum(annual_commission) over (
                partition by transaction_date, transaction_type, policy_number
            ) as annual_commission,
            sum(net_rated_premium_due_to_underwriter) over (
                partition by transaction_date, transaction_type, policy_number
            ) as net_rated_premium_due_to_underwriter,
            sum(total_due_to_underwriter) over (
                partition by transaction_date, transaction_type, policy_number
            ) as total_due_to_underwriter,
            transaction_at = max(transaction_at) over (
                partition by transaction_date, transaction_type, policy_number
                order by transaction_at desc
            ) as _include
        from all_transactions
    )
select
    * except (
        annual_commission,
        net_rated_premium_due_to_underwriter,
        total_due_to_underwriter,
        _include
    ),
    round(annual_commission, 2, "ROUND_HALF_EVEN") as annual_commission,
    round(
        net_rated_premium_due_to_underwriter, 2, "ROUND_HALF_EVEN"
    ) as net_rated_premium_due_to_underwriter,
    round(total_due_to_underwriter, 2, "ROUND_HALF_EVEN") as total_due_to_underwriter
from agg_differences_by_day
where _include = true
order by transaction_at
