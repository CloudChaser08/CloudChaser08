SELECT
    CONCAT(proc.client_id, '_', proc_pay.patientid)        AS claim_id,
    proc.input_file_name                                 AS data_set,
    demo_pay.hvid                                        AS hvid,
    CASE
    WHEN SUBSTRING(COALESCE(demo_pay.gender, demo.sex), 1, 1) IN ('M', 'F')
    THEN SUBSTRING(COALESCE(demo_pay.gender, demo.sex), 1, 1) ELSE 'U'
    END                                                  AS patient_gender,
    demo_pay.yearOfBirth                                 AS patient_year_of_birth,
    demo_pay.threeDigitZip                               AS patient_zip3,
    UPPER(COALESCE(demo_pay.state, demo.pt_st_id))       AS patient_state,
    EXTRACT_DATE(
        demo.dos, '%m/%d/%Y'
        )                                                AS date_service,
    proc.proc_code                                       AS procedure_code,
    ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_3
        )[n.n]                                           AS diagnosis_code
FROM billed_procedures proc
    INNER JOIN billed_procedures_payload proc_pay ON proc.hvjoinkey = proc_pay.hvjoinkey
    LEFT JOIN demographics_payload demo_pay ON proc_pay.patientid = demo_pay.patientid
    LEFT JOIN demographics demo ON demo_pay.hvjoinkey = demo.hvjoinkey
    CROSS JOIN diag_exploder n
WHERE ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_3
        )[n.n] IS NOT NULL
    OR (
        COALESCE(
            proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_3
            ) IS NULL AND n.n = 0
        )
