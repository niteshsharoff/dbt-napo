{% macro agg(value,type='sum') %}
  {%if type=='sum'%}
    sum(cast({{value}} as numeric)) as {{value}}
  {% elif type=='count' %}
    count({{value}}) as {{value}}
  {% endif %}
{% endmacro %}
