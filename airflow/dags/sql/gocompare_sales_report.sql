select * except (_transaction_at)
from {{ source_table_name }}
where _transaction_at >= '{{ start_date }}' and _transaction_at < '{{ end_date }}'
