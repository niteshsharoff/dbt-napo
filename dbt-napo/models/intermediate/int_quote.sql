with
    quote as (
        select
            *,
            split(
                json_extract_scalar(pricing_model_response, '$.pricing_model_version'),
                ' '
            )[3] as pricing_model_commit_hash,
            substr(
                json_extract_scalar(pricing_service_response, '$.version'), 2
            ) as pricing_service_version
        from {{ source("raw", "quoterequest") }}
    )
select
    quote.* except (quote_request_id),
    quote.quote_request_id as quote_id,
    pricing_model_version.pricing_model_version as pricing_model_version,
    coalesce(
        pricing_service_version,
        regexp_extract(
            pricing_model_version.pricing_model_version, "^([0-9]+(?:\\.[0-9]+)?)"
        )
    ) as pricing_algorithm_version
from quote
left join
    {{ ref("pricing_model_version") }}
    on quote.pricing_model_commit_hash = pricing_model_version.pricing_model_commit_hash
