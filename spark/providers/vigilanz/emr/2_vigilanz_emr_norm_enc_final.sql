SELECT  MONOTONICALLY_INCREASING_ID()  AS row_id ,*
FROM

(
SELECT * FROM vigilanz_emr_norm_enc_dedupe

)
--limit 1
