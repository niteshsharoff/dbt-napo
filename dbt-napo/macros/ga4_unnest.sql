{%- macro ga4_unnest(field, new_name=None, array="event_params") -%}
    {% set new_name = new_name if new_name else field %}
    {% if array == "event_params" %}
        (
            select
                coalesce(
                    value.string_value,
                    cast(value.int_value as string),
                    cast(value.double_value as string),
                    cast(value.float_value as string)
                )
            from unnest(event_params)
            where key = '{{ field }}'
        ) as {{ new_name | safe }}
    {% endif %}
{% endmacro %}
