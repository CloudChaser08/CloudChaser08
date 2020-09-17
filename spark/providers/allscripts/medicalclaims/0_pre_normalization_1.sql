SELECT
    sln.service_charge_line_number AS line_number,
    clm.*,
    sln.*,
    pay.*,
    linked_and_unlinked_diagnoses(
        ARRAY(
            clm.header_diagnosis_code_1,
            clm.header_diagnosis_code_2,
            clm.header_diagnosis_code_3,
            clm.header_diagnosis_code_4,
            clm.header_diagnosis_code_5,
            clm.header_diagnosis_code_6,
            clm.header_diagnosis_code_7,
            clm.header_diagnosis_code_8
        ),
        ARRAY(
            sln.service_diagnosis_code_pointer_1,
            sln.service_diagnosis_code_pointer_2,
            sln.service_diagnosis_code_pointer_3,
            sln.service_diagnosis_code_pointer_4
        )
    )                               AS linked_diagnoses
FROM header clm
LEFT JOIN serviceline sln
    ON clm.header_entity_id = sln.service_entity_id
LEFT JOIN matching_payload pay
    ON clm.header_hvJoinKey = pay.hvJoinKey
