SELECT monotonically_increasing_id() AS row_id, *
FROM
    (
        SELECT * FROM practice_fusion_emr_norm_emr_clin_obsn_1
        UNION ALL
        SELECT * FROM practice_fusion_emr_norm_emr_clin_obsn_2
        UNION ALL
        SELECT * FROM practice_fusion_emr_norm_emr_clin_obsn_3
    )
