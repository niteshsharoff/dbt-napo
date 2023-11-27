{{ config(schema="marts", materialized="table") }}

with
    customer_scd as (
        select
            customer_id,
            uuid as customer_uuid,
            first_name,
            last_name,
            email,
            street_address,
            address_locality,
            address_region,
            postal_code,
            date_of_birth,
            change_reason,
            effective_from,
            effective_to
        from {{ ref("stg_raw__customer_ledger") }} customer
        left join {{ source("raw", "user") }} user on customer.user_id = user.id
    ),
    compute_row_hash as (
        select
            *,
            farm_fingerprint(
                concat(
                    customer_id,
                    customer_uuid,
                    coalesce(first_name, ''),
                    coalesce(last_name, ''),
                    coalesce(email, ''),
                    coalesce(street_address, ''),
                    coalesce(address_locality, ''),
                    coalesce(address_region, ''),
                    coalesce(postal_code, ''),
                    cast(date_of_birth as string),
                    coalesce(change_reason, '')
                )
            ) as row_hash
        from customer_scd
    ),
    tag_changes as (
        select
            *,
            -- identify rows that have changed
            coalesce(
                row_hash <> lag(row_hash) over (
                    partition by customer_uuid order by effective_from
                ),
                true
            ) as has_changed
        from compute_row_hash
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            sum(cast(has_changed as integer)) over (
                partition by customer_uuid order by effective_from
            ) as grp_id
        from tag_changes
    ),
    final as (
        select
            customer_id,
            customer_uuid,
            first_name,
            last_name,
            email,
            street_address,
            address_locality,
            address_region,
            postal_code,
            date_of_birth,
            change_reason,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to
        from assign_grp_id
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, grp_id
        order by customer_uuid, effective_from
    )
select *
from final
