with
    quote_data as (
        select
            quote_request_id as quote_id,
            -- ,JSON_value(common_quote,"$.id") as quote_id
            timestamp_millis(created_at) as created_at,
            date(run_date) as run_date,
            source,
            state,
            discount_percent,
            discount_type,
            -- ,common_quote
            json_value(common_quote, "$.discount") as discount,
            -- ,JSON_value(common_quote,"$.source") as source
            date(json_value(common_quote, "$.start_date")) as start_date,
            json_value(common_quote, "$.voucher_code") as voucher_code,
            struct(
                json_value(common_quote, "$.customer.first_name") as first_name,
                json_value(common_quote, "$.customer.last_name") as last_name,
                json_value(common_quote, "$.customer.email_address") as email_address,
                date(
                    json_value(common_quote, "$.customer.date_of_birth")
                ) as date_of_birth,
                json_value(
                    common_quote, "$.customer.address_locality"
                ) as address_locality,
                json_value(common_quote, "$.customer.address_region") as address_region,
                json_value(common_quote, "$.customer.phone_number") as phone_number,
                json_value(common_quote, "$.customer.street_address") as street_address,
                json_value(common_quote, "$.customer.postal_code") as postal_code
            ) as customer,
            json_query_array(common_quote, "$.pets") as pets,
            json_query_array(common_quote, "$.products") as products
        from {{ source("raw", "quoterequest") }}
    )
select *
from quote_data
