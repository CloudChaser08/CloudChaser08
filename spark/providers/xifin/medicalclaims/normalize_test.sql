SELECT
    CONCAT(test.full_accn_id)                            AS claim_id,
    test.input_file_name                                 AS data_set,
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
    test.proc_code                                       AS procedure_code
FROM ordered_tests_complete test
    LEFT JOIN demographics_complete demo ON test.full_accn_id = demo.full_accn_id
