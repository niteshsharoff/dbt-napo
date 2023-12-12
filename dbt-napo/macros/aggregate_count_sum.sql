{% macro agg(value,renamed_value=None,type='sum') %}
  {%if renamed_value==None%}
    {%set renamed_value = value%}
  {%endif%}
  {%if type=='sum'%}
    sum(cast({{value}} as numeric)) as {{value}}
  {% elif type=='count' %}
    count({{value}}) as {{renamed_value}}
  {% endif %}
{% endmacro %}
