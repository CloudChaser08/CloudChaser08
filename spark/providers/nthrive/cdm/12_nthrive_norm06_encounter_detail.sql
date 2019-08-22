SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    sub.*
 FROM
(
    SELECT *
     FROM nthrive_norm03_encounter_detail_1
    UNION ALL
    SELECT *
     FROM nthrive_norm04_encounter_detail_2
    UNION ALL
    SELECT *
     FROM nthrive_norm05_encounter_detail_3
) sub