select a.*
--    ,b.source_breed_name
      ,b.common_breed_name 
from {{ref('stg_postgres__breed')}} a
left join {{ref('stg_postgres__breed_mapping')}} b
on a.name = b.source_breed_name