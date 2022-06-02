select pk
      ,fields.*
from {{source('raw','breed_mapping')}}