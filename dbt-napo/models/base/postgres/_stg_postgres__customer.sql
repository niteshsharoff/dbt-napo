select
    pk,
    fields.* except (_user_id, _customerid),
    fields._user_id as user,
    fields._customerid as customer,
    /*except(mandate_inactive_event_id
                      ,address_region
                      ,street_address
                      ,phone_number
                      ,postal_code
                      ,date_of_birth
                      ,address_locality)*/
    extract(year from fields.date_of_birth) as year_of_birth
from {{ source("postgres", "customer") }}
