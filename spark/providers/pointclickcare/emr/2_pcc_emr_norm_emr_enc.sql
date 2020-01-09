SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    sub.*
 FROM
(
    SELECT *
     FROM pcc_emr_norm_emr_enc_1
    UNION ALL
    SELECT *
     FROM pcc_emr_norm_emr_enc_2
) sub
