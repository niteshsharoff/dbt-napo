/*
  GIVEN
    our training user level metrics table

  WHEN
    the customer_uuid is not null

  THEN
    we expect the number rows per customer to be one
*/
with
    q1 as (
        select *, count(email) over (partition by customer_uuid) as cnt
        from {{ ref("training_user_level_metrics") }}
        where customer_uuid is not null
    )
select customer_uuid, email
from q1
where cnt > 1
