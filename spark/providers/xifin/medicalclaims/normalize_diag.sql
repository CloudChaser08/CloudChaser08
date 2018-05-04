SELECT
    CONCAT(diag.client_id, '_', diag_pay.accn_id)        AS claim_id,
    diag.input_file_name                                 AS data_set,
    demo_pay.hvid                                        AS hvid,
    CASE
    WHEN COALESCE(demo_pay.gender, demo.sex) IN ('M', 'F')
    THEN COALESCE(demo_pay.gender, demo.sex)
    ELSE 'U'
    END                                                  AS patient_gender,
    demo_pay.yearOfBirth                                 AS patient_year_of_birth,
    demo_pay.threeDigitZip                               AS patient_zip3,
    UPPER(COALESCE(demo_pay.state, demo.pt_st_id))       AS patient_state,
    EXTRACT_DATE(
        demo.dos, '%m/%d/%Y'
        )                                                AS date_service,
    diag.diag_code                                       AS diagnosis_code,
    CASE
    WHEN UPPER(diag.diagnosis_code_table) LIKE 'ICD-9%' THEN '01'
    WHEN UPPER(diag.diagnosis_code_table) LIKE 'ICD-10%' THEN '02'
    END                                                  AS diagnosis_code_qual
FROM
    account_diagnosis diag
    INNER JOIN account_diagnosis_payload diag_pay ON diag.hvjoinkey = diag_pay.hvjoinkey
    LEFT JOIN demographics_payload demo_pay ON diag_pay.accn_id = demo_pay.accn_id
    LEFT JOIN demographics demo ON demo_pay.hvjoinkey = demo.hvjoinkey
