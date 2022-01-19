SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM
(  
SELECT  *
FROM amazingcharts_clin_obsn_pre_final_norm
)
