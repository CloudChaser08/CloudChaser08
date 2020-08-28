SELECT
    *
 FROM patient_diagnosis dgn1
WHERE TRIM(dgn1.code_type) <> 'A'
/* Add all admitting diagnosis codes where the diagnosis code doesn't exist */
/* in the list for that record_id, without the admitting diagnosis indicator. */
UNION ALL
SELECT
    *
 FROM patient_diagnosis dgn2
WHERE TRIM(dgn2.code_type) = 'A'
  AND NOT EXISTS
    (
        SELECT 1
         FROM patient_diagnosis dgn3
        WHERE TRIM(dgn3.code_type) <> 'A'
          AND TRIM(dgn2.record_id) = TRIM(dgn3.record_id)
          AND TRIM(dgn2.icd_diagnosis_code) = TRIM(dgn3.icd_diagnosis_code)
    )
