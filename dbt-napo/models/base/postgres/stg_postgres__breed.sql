select pk
      ,fields.*
from {{source('postgres','breed')}}