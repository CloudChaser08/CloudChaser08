SELECT
    CONCAT(proc.full_accn_id)                            AS claim_id,
    proc.input_file_name                                 AS data_set,
    demo.hvid                                            AS hvid,
    CASE
    WHEN SUBSTRING(COALESCE(demo.gender, demo.sex), 1, 1) IN ('M', 'F')
    THEN SUBSTRING(COALESCE(demo.gender, demo.sex), 1, 1) ELSE 'U'
    END                                                  AS patient_gender,
    demo.yearOfBirth                                     AS patient_year_of_birth,
    demo.threeDigitZip                                   AS patient_zip3,
    UPPER(COALESCE(demo.state, demo.pt_st_id))           AS patient_state,
    EXTRACT_DATE(
        demo.dos, '%m/%d/%Y'
        )                                                AS date_service,
    proc.proc_code                                       AS procedure_code,
    ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_3
        )[n.n]                                           AS diagnosis_code
FROM billed_procedures_complete proc
    LEFT JOIN demographics_complete demo ON proc.full_accn_id = demo.full_accn_id
    CROSS JOIN diag_exploder n
WHERE ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_3
        )[n.n] IS NOT NULL
    OR (
        COALESCE(
            proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_3
            ) IS NULL AND n.n = 0
        )
