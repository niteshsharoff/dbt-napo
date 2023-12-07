{{ config(materialized="table") }}

with
    -- Bad data model, voucher_code_id embedded in policy & quote_id embedded in quote
    quote_voucher_ids as (
        select distinct quote_id, voucher_code_id
        from {{ ref("stg_raw__policy_ledger") }}
        where voucher_code_id is not null
    ),
    napobenefitcode as (
        select
            quote_id as quote_uuid, code as benefit_code, reward as reward_amount_pounds
        from {{ ref("stg_raw__quotewithbenefit") }} q
        join {{ ref("stg_raw__napobenefitcode") }} b on q.benefit_id = b.id
    ),
    vouchercode as (
        select quote_id as quote_uuid, voucher_id, voucher_code, discount_percentage
        from {{ ref("stg_raw__vouchercode") }}
        where quote_id is not null
    ),
    promotion as (
        select
            quote_uuid,
            voucher_id,
            voucher_code,
            discount_percentage,
            null as benefit_code,
            null as reward_amount_pounds
        from vouchercode
        union distinct
        select
            quote_uuid,
            null as voucher_id,
            null as voucher_code,
            null as discount_percentage,
            benefit_code,
            reward_amount_pounds
        from napobenefitcode
    ),
    renewal as (
        select quote_id, old_policy_id
        from
            (
                -- Use only the latest renewal row
                select
                    *,
                    updated_at = max(updated_at) over (
                        partition by uuid order by updated_at desc
                    ) as _is_latest
                from {{ ref("stg_raw__renewal") }}
            )
        where _is_latest = true
    ),
    final as (
        select
            q.quote_id as quote_uuid,
            case
                when source = 'tungsten-vale'
                then 'gocompare'
                when source = 'gallium-rapid'
                then 'quotezone'
                when source = 'fermium-cliff'
                then 'confused'
                when source = 'indium-river'
                then 'moneysupermarket'
                when source = 'hassium-oxbow'
                then 'comparethemarket'
                else source
            end as quote_source,
            r.old_policy_id as source_policy_id,
            msm_sales_tracking_urn,
            json_extract_scalar(common_quote, '$.discount.type') as discount_type,
            p.* except (quote_uuid, voucher_id),
            to_json_string(raw_request) as raw_request,
            to_json_string(raw_response) as raw_response,
            to_json_string(common_quote) as common_quote,
            to_json_string(pricing_service_request) as pricing_service_request,
            to_json_string(pricing_service_response) as pricing_service_response,
            pricing_model_version,
            pricing_algorithm_version,
            pricing_algorithm_version_with_patch,
            timestamp_millis(created_at) as quote_at,
        from {{ ref("int_policy_quote") }} q
        left join quote_voucher_ids v on q.quote_id = v.quote_id
        left join
            promotion p
            on (q.quote_id = p.quote_uuid or v.voucher_code_id = p.voucher_id)
            and (
                coalesce(json_extract_scalar(common_quote, '$.discount.type'), '')
                not in ('multipet', 'multipet_with_voucher_code')
            )  -- Taken from common_quote
        left join renewal r on q.quote_id = r.quote_id
    )
select *
from final
