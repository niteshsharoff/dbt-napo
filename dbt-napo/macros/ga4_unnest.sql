{%- macro ga4_unnest(field, new_name=None, array='event_params') -%}
    {% set new_name = new_name if new_name else field %}
    {% if array == 'event_params' %}
        (
            SELECT 
                COALESCE(
                    value.string_value,
                    CAST(value.int_value AS STRING),
                    CAST(value.double_value AS STRING),
                    CAST(value.float_value AS STRING)
                ) 
            FROM UNNEST(event_params) 
            WHERE key = '{{ field }}'
        ) AS {{ new_name | safe }}
    {% endif %}
{% endmacro %}
