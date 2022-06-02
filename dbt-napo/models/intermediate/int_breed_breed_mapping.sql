select a.*
--    ,b.source_breed_name
      ,b.common_breed_name 
from {{ref('raw_breed')}} a
left join {{ref('raw_breed_mapping')}} b
on a.name = b.source_breed_name