WITH
    quote AS (
        SELECT
            *,
            split(
                json_extract_scalar(
                    pricing_model_response,
                    '$.pricing_model_version'
                ),
                ' '
            )[3] AS pricing_model_commit_hash
        FROM
            {{ source("raw", "quoterequest") }}
    )
SELECT
    quote.* EXCEPT (quote_request_id),
    quote.quote_request_id AS quote_id,
    pricing_model_version.pricing_model_version AS pricing_model_version
FROM
    quote
LEFT JOIN
    {{ ref("pricing_model_version") }}
ON
    quote.pricing_model_commit_hash = pricing_model_version.pricing_model_commit_hash