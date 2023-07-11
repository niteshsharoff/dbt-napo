with
    metadata as (
        select
            date_diff(
                date('{{ end_date }}'), date('{{ start_date }}'), day
            ) as expected_partition_count
    )
    {%- for source_table in source_tables %}
        ,
        {{ source_table }}_table as (
            select count(distinct(run_date)) as {{ source_table }}_partition_count
            from raw.{{ source_table }}
            where
                (
                    run_date >= date('{{ start_date }}')
                    and run_date < date('{{ end_date }}')
                )
        )
    {%- endfor %}
select *
from
    metadata
    {%- for source_table in source_tables %}, {{ source_table }}_table {%- endfor %}
where
    expected_partition_count != expected_partition_count
    {%- for source_table in source_tables %}
        or {{ source_table }}_partition_count != expected_partition_count
    {%- endfor %}
