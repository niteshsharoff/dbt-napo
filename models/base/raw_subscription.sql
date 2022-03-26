select pk
      ,fields.*
from {{source('raw','subscription')}}