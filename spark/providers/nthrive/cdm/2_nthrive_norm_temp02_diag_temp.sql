SELECT DISTINCT
    record_id,
    icd_diagnosis_code,
    'Y'                                 AS admit_diag_flg
 FROM patient_diagnosis
WHERE COALESCE(code_type, 'X') = 'A'
