with
    q1 as (
        select
            stripe_customer_id,
            count(customer_uuid) over (partition by stripe_customer_id) as cnt
        from {{ ref("int_training_customer") }}
    )
select *
from q1
where cnt > 1
