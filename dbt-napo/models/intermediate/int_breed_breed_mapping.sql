select a.*
--    ,b.source_breed_name
      ,b.common_breed_name 
from {{ref('stg_breed')}} a
left join {{ref('stg_breed_mapping')}} b
on a.name = b.source_breed_name