SELECT 
    MONOTONICALLY_INCREASING_ID()   AS row_id,
    sub.*
 FROM
(
    SELECT * FROM ihm_emr_norm_emr_proc_1
    UNION ALL
    SELECT * FROM ihm_emr_norm_emr_proc_2

) sub
