SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM
(
    SELECT * FROM practice_fusion_emr_norm_emr_dedup_lab_test
)
