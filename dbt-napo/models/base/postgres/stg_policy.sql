select pk
      ,fields.*
from {{source('postgres','policy')}}