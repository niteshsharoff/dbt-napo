with
    a as (
        select
            id,
            firstname as first_name,
            lastname as last_name,
            email,
            phone,
            courses,
            _imported_at
        from {{ source("firestore", "users") }}
    )
select *
from a
