SELECT
    sln.service_charge_line_number  AS line_number,
    clm.*,
    sln.*,
    pay.*,
    ARRAY(
        clm.header_diagnosis_code_1,
        clm.header_diagnosis_code_2,
        clm.header_diagnosis_code_3,
        clm.header_diagnosis_code_4,
        clm.header_diagnosis_code_5,
        clm.header_diagnosis_code_6,
        clm.header_diagnosis_code_7,
        clm.header_diagnosis_code_8
    )                               AS all_diagnoses,
    ldr.linked_diagnoses            AS linked_diagnoses,
    ldr.earliest_service_date       AS earliest_service_date,
    ldr.latest_service_date         AS latest_service_date
FROM header clm
LEFT JOIN serviceline sln
    ON clm.header_entity_id = sln.service_entity_id
    AND service_charge_line_number = '1'
LEFT JOIN matching_payload pay
    ON clm.header_hvJoinKey = pay.hvJoinKey
LEFT JOIN 
    (SELECT claim_id, collect_set(diagnosis_code) linked_diagnoses,
        MIN(date_service) as earliest_service_date,
        MAX(date_service_end) as latest_service_date FROM 
        normalize_1 GROUP BY claim_id) ldr
    ON clm.header_entity_id = ldr.claim_id
