SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM

(
SELECT * FROM tsi_emr_norm_emr_proc_pre_final
)
