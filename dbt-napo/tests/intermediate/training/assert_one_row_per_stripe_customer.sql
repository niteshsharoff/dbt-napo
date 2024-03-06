/*
  GIVEN
    a table of customers for training & socialisation

  WHEN
    we count the number of rows per stripe customer

  THEN
    we expect there to only be one
*/
with
    q1 as (
        select
            stripe_customer_id,
            count(customer_uuid) over (partition by stripe_customer_id) as cnt
        from {{ ref("int_training_customers") }}
        where stripe_customer_id is not null
    )
select *
from q1
where cnt > 1
