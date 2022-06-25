select pk
      ,fields.*
from {{source('raw','breed')}}