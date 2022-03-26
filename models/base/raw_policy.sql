select pk
      ,fields.*
from {{source('raw','policy')}}