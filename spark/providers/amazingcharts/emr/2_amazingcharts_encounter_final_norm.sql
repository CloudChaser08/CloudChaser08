SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM
(
SELECT  *
FROM amazingcharts_encounter_pre_final_norm
)
