{% macro marketing_campaign_classification(field_name) %}
    case
        when lower(trim({{ field_name }})) like '%leadgen%'
        then 'leadgen'
        when lower(trim({{ field_name }})) like '%standalone%'
        then 'pa_standalone'
        when lower(trim({{ field_name }})) not like any ('%leadgen%', '%standalone%')
        then 'growth'
        else 'other'
    end
{% endmacro %}
