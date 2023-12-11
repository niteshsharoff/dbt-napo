{{ config(materialized="table") }}

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
    has_row_changed as (
        select
            *,
            -- coalesce nullable fields
            case
                when
                    customer_id != lag(customer_id) over (
                        partition by customer_uuid order by effective_from
                    )
                then 1
                when
                    customer_uuid != lag(customer_uuid) over (
                        partition by customer_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(first_name, '') != coalesce(
                        lag(first_name) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(last_name, '') != coalesce(
                        lag(last_name) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(email, '') != coalesce(
                        lag(email) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(street_address, '') != coalesce(
                        lag(street_address) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(address_locality, '') != coalesce(
                        lag(address_locality) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(address_region, '') != coalesce(
                        lag(address_region) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    coalesce(postal_code, '') != coalesce(
                        lag(postal_code) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                when
                    date_of_birth != lag(date_of_birth) over (
                        partition by customer_uuid order by effective_from
                    )
                then 1
                when
                    coalesce(change_reason, '') != coalesce(
                        lag(change_reason) over (
                            partition by customer_uuid order by effective_from
                        ),
                        ''
                    )
                then 1
                else 0
            end as row_changed
        from customer_scd
    ),
    assign_grp_id as (
        select
            *,
            -- assign changes to buckets
            coalesce(
                sum(row_changed) over (
                    partition by customer_uuid order by effective_from
                ),
                0
            ) as grp_id
        from has_row_changed
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
        group by
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
            grp_id
        order by customer_uuid, effective_from
    )
select *
from final
