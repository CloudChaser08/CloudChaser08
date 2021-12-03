SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM

(
SELECT * FROM vigilanz_emr_norm_lab_test_dedupe

)
--limit 1
