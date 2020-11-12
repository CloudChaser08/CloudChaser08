SELECT
    MONOTONICALLY_INCREASING_ID()   AS row_id,
    sub.*
 FROM
(
    SELECT * FROM pcc_emr_norm_emr_clin_obsn_1
    UNION ALL SELECT * FROM pcc_emr_norm_emr_clin_obsn_2
    UNION ALL SELECT * FROM pcc_emr_norm_emr_clin_obsn_3
    UNION ALL SELECT * FROM pcc_emr_norm_emr_clin_obsn_4
) sub
