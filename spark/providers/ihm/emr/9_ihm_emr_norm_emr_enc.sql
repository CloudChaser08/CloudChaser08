SELECT 
    MONOTONICALLY_INCREASING_ID()   AS row_id,
    sub.*
 FROM
(
    SELECT * FROM ihm_emr_norm_emr_pre_02_enc

) sub
