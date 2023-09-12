{% macro declare_policy_udfs() %}

    create or replace function
        {{ target.schema }}.is_sold(annual_payment_id string, subscription_id string)
    returns bool
    as ((annual_payment_id is not null) or (subscription_id is not null))
    ;

    create or replace function
        {{ target.schema }}.is_cancelled(
            annual_payment_id string, subscription_id string, cancel_date int64
        )
    returns bool
    as
        (
            ((annual_payment_id is not null) or (subscription_id is not null))
            and cancel_date is not null
        )
    ;

    create or replace function
        {{ target.schema }}.get_breed_for_pet(
            species string, size string, breed_category string, breed_name string
        )
    returns string
    as
        (
            case
                when breed_category = 'mixed' and species = 'cat'
                then 'mixed'
                when breed_category = 'mixed' and species = 'dog' and size is null
                then 'mixed'
                when breed_category = 'mixed' and species = 'dog' and size = '20kg+'
                then 'Large Mongrel (> 20Kg)'
                when breed_category = 'mixed' and species = 'dog' and size = '10-20kg'
                then 'Medium Mongrel (10Kg to 20Kg)'
                when
                    breed_category = 'mixed' and species = 'dog' and size = 'up to 10kg'
                then 'Small Mongrel (< 10 Kg)'
                when breed_category = 'pedigree' and breed_name is null
                then 'unknown'
                when breed_category = 'pedigree' and breed_name is not null
                then breed_name
                when breed_category = 'cross' and breed_name is null
                then 'cross'
                when breed_category = 'cross' and breed_name is not null
                then breed_name || ' + cross'
            end
        )
    ;

{% endmacro %}
