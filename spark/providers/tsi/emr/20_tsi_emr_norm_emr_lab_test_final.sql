SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM
(
    SELECT * FROM tsi_emr_norm_dedup_emr_lab_test_1
)
