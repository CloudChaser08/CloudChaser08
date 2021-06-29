SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    sub.*
 FROM
(
    SELECT *
     FROM darch.nthrive_norm03_encounter_detail_1
    UNION ALL
    SELECT *
     FROM darch.nthrive_norm04_encounter_detail_2
    UNION ALL
    SELECT *
     FROM darch.nthrive_norm05_encounter_detail_3
) sub