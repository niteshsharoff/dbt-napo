select * except(renewal_at, created_at, updated_at)
  , timestamp_millis(renewal_at) as renewal_at
  , timestamp_millis(created_at) as created_at
  , timestamp_millis(updated_at) as updated_at
from {{ source('raw', 'renewal') }}