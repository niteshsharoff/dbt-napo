SELECT
    * EXCEPT(
        behavioural_treatment_cover
    ),
    CAST(behavioural_treatment_cover AS FLOAT64) AS behavioural_treatment_cover

FROM
    {{source('raw','product')}}