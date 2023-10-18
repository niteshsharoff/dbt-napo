{% macro declare_quote_udfs() %}

    create or replace function
        {{ target.schema }}.strip_patch_number(version_string string)
    returns string
    as (regexp_extract(version_string, "^([0-9]+(?:\\.[0-9]+)?)"))
    ;

{% endmacro %}
