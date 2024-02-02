with
    big_query_job_usage as (
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
            total_bytes_billed / 1024 / 1024 / 1024 / 1024 * 6.5 as job_cost_usd,
            timestamp_diff(end_time, start_time, second) as duration,
            query,
            date(creation_time) as created_at,
            start_time,
            end_time,
            total_bytes_processed,
            total_bytes_billed,
            total_slot_ms
        -- fmt: off
        from `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT jobs
        -- fmt: on
    )
select
    created_at,
    destination_table,
    sum(job_cost_usd) as job_cost_usd,
    sum(gb_processed) as gb_processed
from big_query_job_usage
group by 1, 2
order by 1 desc, 3 desc
