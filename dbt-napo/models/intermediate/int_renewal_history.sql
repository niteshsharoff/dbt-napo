with
    renewal_quote as (
        select
            quote,
            json_extract_scalar(common_quote, '$.voucher_code.code') as voucher_code
        from {{ ref("int_renewal_quote") }} as quote

    ),
    renewal_quote_product as (
        select
            quote,
            cast(
                json_extract_scalar(product_price, '$.annual_price') as float64
            ) as annual_retail_price,
            json_extract_scalar(
                product_price, '$.product_reference'
            ) as product_reference
        from
            {{ ref("int_renewal_quote") }} as quote,
            unnest(
                json_extract_array(
                    json_extract_array(common_quote, '$.pets')[0], '$.products'
                )
            ) as product_price
    ),
    renewal as (
        select
            renewal.* except (new_policy_product_id),
            product.reference as new_policy_product_reference,
        from {{ ref("stg_raw__renewal") }} as renewal
        left join
            {{ ref("stg_raw__product") }} as product
            on product.id = renewal.new_policy_product_id
    ),
    joint_history as (
        select
            renewal,
            old_policy_history.policy as old_policy,
            new_policy_history.policy as new_policy,
            old_policy_history.customer,
            old_policy_history.pet,
            greatest(
                renewal.effective_from,
                old_policy_history.row_effective_from,
                coalesce(new_policy_history.row_effective_from, timestamp_millis(0))
            ) as row_effective_from,
            least(
                renewal.effective_to,
                old_policy_history.row_effective_to,
                coalesce(
                    new_policy_history.row_effective_to,
                    timestamp("2999-01-01 00:00:00+00")
                )
            ) as row_effective_to
        from renewal
        left join
            {{ ref("int_policy_history") }} as old_policy_history
            on renewal.old_policy_id = old_policy_history.policy.policy_id
            and old_policy_history.row_effective_to >= renewal.effective_from
            and old_policy_history.row_effective_from < renewal.effective_to
        left join
            {{ ref("int_policy_history") }} as new_policy_history
            on renewal.new_policy_id = new_policy_history.policy.policy_id
            and new_policy_history.row_effective_to >= renewal.effective_from
            and new_policy_history.row_effective_from < renewal.effective_to
    )
select
    renewal_quote_product.quote,
    renewal,
    annual_retail_price,
    voucher_code,
    old_policy,
    new_policy,
    customer,
    pet,
    row_effective_from,
    row_effective_to
from joint_history
left join
    renewal_quote_product
    on renewal.quote_id = renewal_quote_product.quote.quote_id
    and renewal.new_policy_product_reference = renewal_quote_product.product_reference
left join renewal_quote on renewal.quote_id = renewal_quote.quote.quote_id
