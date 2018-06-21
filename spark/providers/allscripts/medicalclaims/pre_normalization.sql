SELECT llr.*
 FROM
(
    -- Linked diagnosis 1 -- load at least one row for each service line
    SELECT
        'llr_1' AS row_type,
        sln.service_entity_id,
        sln.service_charge_line_number AS line_number,
        (CASE WHEN sln.service_diagnosis_code_pointer_1 = '1' THEN clm.header_diagnosis_code_1
              WHEN sln.service_diagnosis_code_pointer_1 = '2' THEN clm.header_diagnosis_code_2
              WHEN sln.service_diagnosis_code_pointer_1 = '3' THEN clm.header_diagnosis_code_3
              WHEN sln.service_diagnosis_code_pointer_1 = '4' THEN clm.header_diagnosis_code_4
              WHEN sln.service_diagnosis_code_pointer_1 = '5' THEN clm.header_diagnosis_code_5
              WHEN sln.service_diagnosis_code_pointer_1 = '6' THEN clm.header_diagnosis_code_6
              WHEN sln.service_diagnosis_code_pointer_1 = '7' THEN clm.header_diagnosis_code_7
              WHEN sln.service_diagnosis_code_pointer_1 = '8' THEN clm.header_diagnosis_code_8
              ELSE NULL
          END) AS linked_diagnosis_code,
        '1' AS diagnosis_code_priority,
        clm.*,
        sln.*,
        pay.*
     FROM serviceline sln
     LEFT OUTER JOIN header clm
       ON sln.service_entity_id = clm.header_entity_id
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
    UNION ALL
    -- Linked diagnosis 2 -- only load if pointer and code are both populated
    SELECT
        'llr_2' AS row_type,
        sln.service_entity_id,
        sln.service_charge_line_number AS line_number,
        (CASE WHEN sln.service_diagnosis_code_pointer_2 = '1' THEN clm.header_diagnosis_code_1
              WHEN sln.service_diagnosis_code_pointer_2 = '2' THEN clm.header_diagnosis_code_2
              WHEN sln.service_diagnosis_code_pointer_2 = '3' THEN clm.header_diagnosis_code_3
              WHEN sln.service_diagnosis_code_pointer_2 = '4' THEN clm.header_diagnosis_code_4
              WHEN sln.service_diagnosis_code_pointer_2 = '5' THEN clm.header_diagnosis_code_5
              WHEN sln.service_diagnosis_code_pointer_2 = '6' THEN clm.header_diagnosis_code_6
              WHEN sln.service_diagnosis_code_pointer_2 = '7' THEN clm.header_diagnosis_code_7
              WHEN sln.service_diagnosis_code_pointer_2 = '8' THEN clm.header_diagnosis_code_8
              ELSE NULL
          END) AS linked_diagnosis_code,
        '2' AS diagnosis_code_priority,
        clm.*,
        sln.*,
        pay.*
     FROM serviceline sln
     LEFT OUTER JOIN header clm
       ON sln.service_entity_id = clm.header_entity_id
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
    WHERE 0 <> LENGTH(TRIM(COALESCE(sln.service_diagnosis_code_pointer_2, '')))
      AND sln.service_diagnosis_code_pointer_2 <> sln.service_diagnosis_code_pointer_1
    UNION ALL
    -- Linked diagnosis 3 -- only load if pointer and code are both populated
    SELECT
        'llr_3' AS row_type,
        sln.service_entity_id,
        sln.service_charge_line_number AS line_number,
        (CASE WHEN sln.service_diagnosis_code_pointer_3 = '1' THEN clm.header_diagnosis_code_1
              WHEN sln.service_diagnosis_code_pointer_3 = '2' THEN clm.header_diagnosis_code_2
              WHEN sln.service_diagnosis_code_pointer_3 = '3' THEN clm.header_diagnosis_code_3
              WHEN sln.service_diagnosis_code_pointer_3 = '4' THEN clm.header_diagnosis_code_4
              WHEN sln.service_diagnosis_code_pointer_3 = '5' THEN clm.header_diagnosis_code_5
              WHEN sln.service_diagnosis_code_pointer_3 = '6' THEN clm.header_diagnosis_code_6
              WHEN sln.service_diagnosis_code_pointer_3 = '7' THEN clm.header_diagnosis_code_7
              WHEN sln.service_diagnosis_code_pointer_3 = '8' THEN clm.header_diagnosis_code_8
              ELSE NULL
          END) AS linked_diagnosis_code,
        '3' AS diagnosis_code_priority,
        clm.*,
        sln.*,
        pay.*
     FROM serviceline sln
     LEFT OUTER JOIN header clm
       ON sln.service_entity_id = clm.header_entity_id
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
    WHERE 0 <> LENGTH(TRIM(COALESCE(sln.service_diagnosis_code_pointer_3, '')))
      AND sln.service_diagnosis_code_pointer_3 <> sln.service_diagnosis_code_pointer_1
      AND sln.service_diagnosis_code_pointer_3 <> sln.service_diagnosis_code_pointer_2
    UNION ALL
    -- Linked diagnosis 4 -- only load if pointer and code are both populated
    SELECT
        'llr_4' AS row_type,
        sln.service_entity_id,
        sln.service_charge_line_number AS line_number,
        (CASE WHEN sln.service_diagnosis_code_pointer_4 = '1' THEN clm.header_diagnosis_code_1
              WHEN sln.service_diagnosis_code_pointer_4 = '2' THEN clm.header_diagnosis_code_2
              WHEN sln.service_diagnosis_code_pointer_4 = '3' THEN clm.header_diagnosis_code_3
              WHEN sln.service_diagnosis_code_pointer_4 = '4' THEN clm.header_diagnosis_code_4
              WHEN sln.service_diagnosis_code_pointer_4 = '5' THEN clm.header_diagnosis_code_5
              WHEN sln.service_diagnosis_code_pointer_4 = '6' THEN clm.header_diagnosis_code_6
              WHEN sln.service_diagnosis_code_pointer_4 = '7' THEN clm.header_diagnosis_code_7
              WHEN sln.service_diagnosis_code_pointer_4 = '8' THEN clm.header_diagnosis_code_8
              ELSE NULL
          END) AS linked_diagnosis_code,
        '4' AS diagnosis_code_priority,
        clm.*,
        sln.*,
        pay.*
     FROM serviceline sln
     LEFT OUTER JOIN header clm
       ON sln.service_entity_id = clm.header_entity_id
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
    WHERE 0 <> LENGTH(TRIM(COALESCE(sln.service_diagnosis_code_pointer_4, '')))
      AND sln.service_diagnosis_code_pointer_4 <> sln.service_diagnosis_code_pointer_1
      AND sln.service_diagnosis_code_pointer_4 <> sln.service_diagnosis_code_pointer_2
      AND sln.service_diagnosis_code_pointer_4 <> sln.service_diagnosis_code_pointer_3
) llr
WHERE llr.row_type = 'llr_1'
   OR 0 <> LENGTH(TRIM(COALESCE(llr.linked_diagnosis_code, '')))

UNION ALL
SELECT clr.*
 FROM
(
    -- Claim-level diagnosis_code_1
    SELECT 
        'clr_1' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_1 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_1, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '1'
             OR sln.service_diagnosis_code_pointer_2 = '1'
             OR sln.service_diagnosis_code_pointer_3 = '1'
             OR sln.service_diagnosis_code_pointer_4 = '1'
            )
    )
    UNION ALL
    -- Claim-level diagnosis_code_2
    SELECT 
        'clr_2' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_2 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_2, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '2'
             OR sln.service_diagnosis_code_pointer_2 = '2'
             OR sln.service_diagnosis_code_pointer_3 = '2'
             OR sln.service_diagnosis_code_pointer_4 = '2'
            )
    )
      AND clm.header_diagnosis_code_2 <> clm.header_diagnosis_code_1
    UNION ALL
    -- Claim-level diagnosis_code_3
    SELECT 
        'clr_3' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_3 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_3, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '3'
             OR sln.service_diagnosis_code_pointer_2 = '3'
             OR sln.service_diagnosis_code_pointer_3 = '3'
             OR sln.service_diagnosis_code_pointer_4 = '3'
            )
    )
      AND clm.header_diagnosis_code_3 <> clm.header_diagnosis_code_1
      AND clm.header_diagnosis_code_3 <> clm.header_diagnosis_code_2
    UNION ALL
    -- Claim-level diagnosis_code_4
    SELECT 
        'clr_4' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_4 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_4, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '4'
             OR sln.service_diagnosis_code_pointer_2 = '4'
             OR sln.service_diagnosis_code_pointer_3 = '4'
             OR sln.service_diagnosis_code_pointer_4 = '4'
            )
    )
      AND clm.header_diagnosis_code_4 <> clm.header_diagnosis_code_1
      AND clm.header_diagnosis_code_4 <> clm.header_diagnosis_code_2
      AND clm.header_diagnosis_code_4 <> clm.header_diagnosis_code_3
    UNION ALL
    -- Claim-level diagnosis_code_5
    SELECT 
        'clr_5' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_5 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_5, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '5'
             OR sln.service_diagnosis_code_pointer_2 = '5'
             OR sln.service_diagnosis_code_pointer_3 = '5'
             OR sln.service_diagnosis_code_pointer_4 = '5'
            )
    )
      AND clm.header_diagnosis_code_5 <> clm.header_diagnosis_code_1
      AND clm.header_diagnosis_code_5 <> clm.header_diagnosis_code_2
      AND clm.header_diagnosis_code_5 <> clm.header_diagnosis_code_3
      AND clm.header_diagnosis_code_5 <> clm.header_diagnosis_code_4
    UNION ALL
    -- Claim-level diagnosis_code_6
    SELECT 
        'clr_6' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_6 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_6, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '6'
             OR sln.service_diagnosis_code_pointer_2 = '6'
             OR sln.service_diagnosis_code_pointer_3 = '6'
             OR sln.service_diagnosis_code_pointer_4 = '6'
            )
    )
      AND clm.header_diagnosis_code_6 <> clm.header_diagnosis_code_1
      AND clm.header_diagnosis_code_6 <> clm.header_diagnosis_code_2
      AND clm.header_diagnosis_code_6 <> clm.header_diagnosis_code_3
      AND clm.header_diagnosis_code_6 <> clm.header_diagnosis_code_4
      AND clm.header_diagnosis_code_6 <> clm.header_diagnosis_code_5
    UNION ALL
    -- Claim-level diagnosis_code_7
    SELECT 
        'clr_7' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_7 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_7, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '7'
             OR sln.service_diagnosis_code_pointer_2 = '7'
             OR sln.service_diagnosis_code_pointer_3 = '7'
             OR sln.service_diagnosis_code_pointer_4 = '7'
            )
    )
      AND clm.header_diagnosis_code_7 <> clm.header_diagnosis_code_1
      AND clm.header_diagnosis_code_7 <> clm.header_diagnosis_code_2
      AND clm.header_diagnosis_code_7 <> clm.header_diagnosis_code_3
      AND clm.header_diagnosis_code_7 <> clm.header_diagnosis_code_4
      AND clm.header_diagnosis_code_7 <> clm.header_diagnosis_code_5
      AND clm.header_diagnosis_code_7 <> clm.header_diagnosis_code_6
    UNION ALL
    -- Claim-level diagnosis_code_8
    SELECT 
        'clr_8' AS row_type,
        clm.header_entity_id AS clm_entity_id,
        NULL AS line_number,
        clm.header_diagnosis_code_8 AS linked_diagnosis_code,
        NULL AS diagnosis_code_priority,
        clm.*,
        sl1.*,
        pay.*
     FROM header clm
     LEFT OUTER JOIN matching_payload pay
       ON clm.header_hvJoinKey = pay.hvJoinKey
     LEFT OUTER JOIN serviceline sl1
       ON 0 = 1
    WHERE 0 <> LENGTH(TRIM(COALESCE(header_diagnosis_code_8, '')))
      AND NOT EXISTS
    (
        SELECT 1
         FROM serviceline sln
        WHERE clm.header_entity_id = sln.service_entity_id
          AND 
            (
                sln.service_diagnosis_code_pointer_1 = '8'
             OR sln.service_diagnosis_code_pointer_2 = '8'
             OR sln.service_diagnosis_code_pointer_3 = '8'
             OR sln.service_diagnosis_code_pointer_4 = '8'
            )
    )
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_1
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_2
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_3
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_4
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_5
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_6
      AND clm.header_diagnosis_code_8 <> clm.header_diagnosis_code_7
) clr
