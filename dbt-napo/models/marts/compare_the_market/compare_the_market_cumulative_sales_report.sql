{{ config(schema="marts") }}

with
    quote_products as (
        select
            quote.quote_id,
            quote.original_selected_product_reference,
            cast(
                json_extract_scalar(product_price, '$.annual_price') as float64
            ) as annual_price,
            json_extract_scalar(
                product_price, '$.product_reference'
            ) as product_reference,
            source
        from
            {{ ref("int_policy_quote") }} as quote,
            unnest(
                json_extract_array(
                    json_extract_array(common_quote, '$.pets')[0], '$.products'
                )
            ) as product_price
    ),
    quote_product as (
        select quote_id, annual_price, original_selected_product_reference, source
        from quote_products
        where product_reference = original_selected_product_reference
    ),
    report as (
        select
            "Napo Limited" as partner_name,
            case
                when product.reference = "essential4K"
                then "Napo Comfort"
                when product.reference = "advanced"
                then "Napo Balance"
                when product.reference = "harmony"
                then "Napo Harmony"
                when product.reference = "serenity"
                then "Napo Serenity"
            end as brand_name,
            case
                when product.reference = "essential4K"
                then "NAP1"
                when product.reference = "advanced"
                then "NAP2"
                when product.reference = "harmony"
                then "NAP3"
                when product.reference = "serenity"
                then "NAP4"
            end as brand_code,
            "CTM" as aggregator_name,
            "PT" as product_code,
            "Lifetime" as product_detail,
            quote.quote_id as partner_quote_reference,
            null as ctm_clickthroughid,
            quote.quote_id as internal_quote_reference,
            policy.reference_number as customer_reference_number,
            policy.reference_number as transaction_reference,
            format_datetime(
                "%d/%m/%Y %H:%M:%S", datetime(policy.sold_at)
            ) as transaction_datetime,
            format_date("%d/%m/%Y", policy.start_date) as product_start_date,
            format_date("%d/%m/%Y", policy.end_date) as product_end_date,
            customer.first_name as first_name,
            customer.last_name as surname,
            customer.email as email,
            format_date("%d/%m/%Y", customer.date_of_birth) as date_of_birth,
            customer.postal_code as post_code,
            coalesce(quote_product.annual_price, policy.annual_price) as quoted_cost,
            policy.annual_price as sold_cost,
            case
                when policy.payment_plan_type = "annually"
                then "Annual"
                when policy.payment_plan_type = "monthly"
                then "Installments"
            end as payment_type,
            "Online" as customer_purchase_method,
            pet.name as pet_name,
            initcap(pet.species) as pet_type,
            case
                when pet.breed_name is not null
                then pet.breed_name
                when pet.size = "up to 10kg"
                then "small - up to 10kg"
                when pet.size = "10-20kg"
                then "medium - 10-20kg"
                when pet.size = "20kg+"
                then "large - over 20kg"
            end as pet_breed,
            1 as number_of_pets,
            case
                when transaction_type = "Cancellation"
                then "Cancellation"
                when transaction_type = "New Policy"
                then "Sale"
            end as transaction_type,
            case
                when transaction_type = "Cancellation"
                then format_datetime("%d/%m/%Y %H:%M:%S", transaction_at)
            end as cancellation_transaction_datetime,
            case
                when transaction_type = "Cancellation"
                then format_datetime("%d/%m/%Y %H:%M:%S", policy.cancel_date)
            end as cancellation_date_effective_date
        from {{ ref("reporting_policy_transaction") }}
        left join quote_product on quote_product.quote_id = quote.quote_id
        where
            quote.source = 'hassium-oxbow'
            and transaction_type in ("New Policy", "Cancellation")
        order by transaction_at
    )
select *
from report
