with
    query_metadata as (
        select
            project_id,
            job_type,
            statement_type,
            concat(
                destination_table.project_id,
                '.',
                destination_table.dataset_id,
                '.',
                destination_table.table_id
            ) as destination_table,
            total_bytes_processed / 1024 / 1024 / 1024 as gb_processed,
            -- See: https://cloud.google.com/bigquery/pricing#queries, 6.25 USD per TB
            -- queried
            total_bytes_billed / 1024 / 1024 / 1024 / 1024 * 6.25 as job_cost_usd,
            timestamp_diff(end_time, start_time, second) as duration,
            -- , query
            date(creation_time) as created_at,
            start_time,
            end_time,
            total_bytes_processed,
            total_bytes_billed,
            total_slot_ms,
            cache_hit
        from `region-eu`.information_schema.jobs_by_project jobs
    )
select *
from query_metadata
order by total_bytes_billed desc
