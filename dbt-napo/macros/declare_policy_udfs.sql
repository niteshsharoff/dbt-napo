{% macro declare_policy_udfs () %}

    create or replace function
        {{ target.schema }}.is_sold(annual_payment_id string, subscription_id string)
    returns bool
    as ((annual_payment_id is not null) or (subscription_id is not null))
    ;

    create or replace function
        {{ target.schema }}.is_cancelled(annual_payment_id string, subscription_id string, cancel_date int64)
    returns bool
    as (((annual_payment_id is not null) or (subscription_id is not null)) and cancel_date is not null)
    ;

{% endmacro %}
