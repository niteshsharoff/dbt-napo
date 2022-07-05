SELECT pk as payment_id
    ,fields.*
FROM {{source('postgres','policy_payment')}}