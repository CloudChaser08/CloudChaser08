SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    sub.*
 FROM
(
    SELECT *
     FROM nthrive_norm_temp05_encounter_detail_temp
    UNION ALL
    SELECT *
     FROM nthrive_norm_temp06_encounter_detail_temp 
    UNION ALL
    SELECT *
     FROM nthrive_norm_temp07_encounter_detail_temp 
) sub
