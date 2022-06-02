select a.*
--    ,b.source_breed_name
      ,b.common_breed_name 
from {{ref('int_pet_breed')}} a
left join {{ref('raw_breed_mapping')}} b
on a.breed_name = b.source_breed_name