select pk
      ,fields.*
from {{source('raw','pet')}}