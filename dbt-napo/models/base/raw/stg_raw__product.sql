select
    * except (behavioural_treatment_cover),
    cast(behavioural_treatment_cover as float64) as behavioural_treatment_cover

from {{ source("raw", "product") }}
