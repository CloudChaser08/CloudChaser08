SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM
(  
SELECT  *
FROM amazingcharts_medication_pre_final_norm
)
