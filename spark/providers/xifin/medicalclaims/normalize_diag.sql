SELECT
    CONCAT(diag.full_accn_id)                            AS claim_id,
    diag.input_file_name                                 AS data_set,
    demo.hvid                                            AS hvid,
    CASE
        WHEN SUBSTRING(COALESCE(demo.gender, demo.sex), 1, 1) IN ('M', 'F')
        THEN SUBSTRING(COALESCE(demo.gender, demo.sex), 1, 1)
    ELSE 'U'
    END                                                  AS patient_gender,
    demo.yearOfBirth                                     AS patient_year_of_birth,
    demo.threeDigitZip                                   AS patient_zip3,
    UPPER(COALESCE(demo.state, demo.pt_st_id))           AS patient_state,
    EXTRACT_DATE(
        demo.dos, '%m/%d/%Y'
        )                                                AS date_service,
    diag.diag_code                                       AS diagnosis_code,
    CASE
    WHEN UPPER(diag.diagnosis_code_table) LIKE 'ICD-9%' THEN '01'
    WHEN UPPER(diag.diagnosis_code_table) LIKE 'ICD-10%' THEN '02'
    END                                                  AS diagnosis_code_qual
FROM
    diagnosis_complete diag
    LEFT JOIN demographics_complete demo ON diag.full_accn_id = demo.full_accn_id
