select
{% for column_name in columns %}
    {{ column_name }} {{ ',' if not loop.last else '' }}
{% endfor %}
from {{ table_name }}
{% if date_column != None %}
where {{ date_column }} >= date('{{ start_date }}') and {{ date_column }} < date('{{ end_date }}')
order by {{ date_column }} desc
{% endif %}
;