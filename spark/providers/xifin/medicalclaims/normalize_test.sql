SELECT
    CONCAT(test.client_id, '_', test_pay.patientid)        AS claim_id,
    test.input_file_name                                 AS data_set,
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
    test.proc_code                                       AS procedure_code
FROM ordered_tests test
    INNER JOIN ordered_tests_payload test_pay ON test.hvjoinkey = test_pay.hvjoinkey
    LEFT JOIN demographics_payload demo_pay ON test_pay.patientid = demo_pay.patientid
    LEFT JOIN demographics demo ON demo_pay.hvjoinkey = demo.hvjoinkey
