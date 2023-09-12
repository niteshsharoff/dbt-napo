select * except (_transaction_at)
from dbt_marts.msm_cumulative_sales_report
where _transaction_at >= '{{ start_date }}' and _transaction_at < '{{ end_date }}'
