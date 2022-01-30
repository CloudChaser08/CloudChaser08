SELECT
    CONCAT(q.accn_id, '_', q.dosid)                                         AS claim_id,
    COALESCE(prov_mp.hvid, mp.hvid)                                         AS hvid,
    CURRENT_DATE() AS created,
    '3'AS model_version,
    SPLIT(q.input_file_name, '/')[SIZE(SPLIT(q.input_file_name, '/')) - 1]  AS data_set,
    '18'                                                                    AS data_feed,
    '7'                                                                     AS data_vendor,
    '1'                                                                     AS source_version,
    CASE
        WHEN prov_mp.hvid IS NOT NULL
            THEN COALESCE(prov_mp.gender, mp.gender)
        ELSE mp.gender
    END                                                                     AS patient_gender,
    CASE
        WHEN prov_mp.hvid IS NOT NULL
            THEN COALESCE(prov_mp.age, mp.age)
        ELSE mp.age
    END                                                                     AS patient_age,
    CAP_YEAR_OF_BIRTH
	(
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.age, mp.age)
            ELSE mp.age
        END,
        TO_DATE(q.date_of_service, 'yyyyMMdd'),
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.yearOfBirth, mp.yearOfBirth)
            ELSE mp.yearOfBirth
        END
    )                                                                       AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.threeDigitZip, mp.threeDigitZip)
            ELSE mp.threeDigitZip
        END, 1, 3)
    )                                                                       AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.state, mp.state)
            ELSE mp.state
        END )
    )                                                                       AS patient_state,
    CASE
        WHEN TO_DATE(q.date_of_service, 'yyyyMMdd') < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(q.date_of_service, 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
        ELSE TO_DATE(q.date_of_service, 'yyyyMMdd')
    END                                                                     AS date_service,
    CASE
        WHEN TO_DATE(q.date_collected, 'yyyyMMdd') < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(q.date_collected, 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
        ELSE TO_DATE(q.date_collected, 'yyyyMMdd')
    END                                                                     AS date_specimen,
    CLEAN_UP_LOINC_CODE(q.loinc_code)                                       AS loinc_code,
    q.lab_id                                                                AS lab_id,
    q.local_order_code                                                      AS test_ordered_local_id,
    q.standard_order_code                                                   AS test_ordered_std_id,
    q.order_name                                                            AS test_ordered_name,
    q.local_result_code                                                     AS result_id,
    q.result_name                                                           AS result_name,
    CLEAN_UP_DIAGNOSIS_CODE (
        SPLIT(q.diagnosis_code, '\\^')[diagnosis_exploder.n]
        ,CASE q.icd_codeset_ind WHEN '9' THEN '01' WHEN '0' THEN '02' END
        ,TO_DATE(q.date_of_service, 'yyyyMMdd')
    )                                                                       AS diagnosis_code,
    CASE q.icd_codeset_ind WHEN '9' THEN '01' WHEN '0' THEN '02' END        AS diagnosis_code_qual,
    CLEAN_UP_NPI_CODE(q.npi)                                                AS ordering_npi,
    q.acct_zip                                                              AS ordering_zip,
    'quest'                                                                 AS part_provider,
    /* part_best_date */
    CASE
        WHEN CAST(EXTRACT_DATE(q.date_of_service, 'yyyyMMdd') AS DATE) < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(q.date_of_service, 'yyyyMMdd') AS DATE) > CAST('{VDR_FILE_DT}' AS DATE) THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT (SUBSTR(q.date_of_service, 1, 4), '-', SUBSTR(q.date_of_service, 5, 2))
    END                                                                 AS part_best_date
FROM quest_labtest_base q
    LEFT JOIN matching_payload mp ON
        q.accn_id = mp.claimid AND q.hv_join_key = mp.hvJoinKey
    LEFT JOIN augmented_with_prov_attrs_mp prov_mp ON q.accn_id = prov_mp.claimId
    CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                                    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                                    28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                                    41, 42, 43, 44, 45, 46, 47, 48, 49 )) AS n) diagnosis_exploder
WHERE
    (SPLIT(TRIM(q.diagnosis_code),'\\^')[diagnosis_exploder.n] IS NOT NULL
        AND SPLIT(TRIM(q.diagnosis_code),'\\^')[diagnosis_exploder.n] != '')

UNION ALL

SELECT
    CONCAT(q.accn_id, '_', q.dosid)                                         AS claim_id,
    COALESCE(prov_mp.hvid, mp.hvid)                                         AS hvid,
    CURRENT_DATE() AS created,
    '3'AS model_version,
    SPLIT(q.input_file_name, '/')[SIZE(SPLIT(q.input_file_name, '/')) - 1]  AS data_set,
    '18'                                                                    AS data_feed,
    '7'                                                                     AS data_vendor,
    '1'                                                                     AS source_version,
    CASE
        WHEN prov_mp.hvid IS NOT NULL
            THEN COALESCE(prov_mp.gender, mp.gender)
        ELSE mp.gender
    END                                                                     AS patient_gender,
    CASE
        WHEN prov_mp.hvid IS NOT NULL
            THEN COALESCE(prov_mp.age, mp.age)
        ELSE mp.age
    END                                                                     AS patient_age,
    CAP_YEAR_OF_BIRTH
	(
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.age, mp.age)
            ELSE mp.age
        END,
        TO_DATE(q.date_of_service, 'yyyyMMdd'),
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.yearOfBirth, mp.yearOfBirth)
            ELSE mp.yearOfBirth
        END
    )                                                                       AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.threeDigitZip, mp.threeDigitZip)
            ELSE mp.threeDigitZip
        END, 1, 3)
    )                                                                       AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(
        CASE
            WHEN prov_mp.hvid IS NOT NULL
                THEN COALESCE(prov_mp.state, mp.state)
            ELSE mp.state
        END )
    )                                                                       AS patient_state,
    CASE
        WHEN TO_DATE(q.date_of_service, 'yyyyMMdd') < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(q.date_of_service, 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
        ELSE TO_DATE(q.date_of_service, 'yyyyMMdd')
    END                                                                     AS date_service,
    CASE
        WHEN TO_DATE(q.date_collected, 'yyyyMMdd') < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(q.date_collected, 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
        ELSE TO_DATE(q.date_collected, 'yyyyMMdd')
    END                                                                     AS date_specimen,
    CLEAN_UP_LOINC_CODE(q.loinc_code)                                       AS loinc_code,
    q.lab_id                                                                AS lab_id,
    q.local_order_code                                                      AS test_ordered_local_id,
    q.standard_order_code                                                   AS test_ordered_std_id,
    q.order_name                                                            AS test_ordered_name,
    q.local_result_code                                                     AS result_id,
    q.result_name                                                           AS result_name,
    NULL                                                                    AS diagnosis_code,
    NULL                                                                    AS diagnosis_code_qual,
    CLEAN_UP_NPI_CODE(q.npi)                                                AS ordering_npi,
    q.acct_zip                                                              AS ordering_zip,
    'quest'                                                                 AS part_provider,
    /* part_best_date */
    CASE
        WHEN TO_DATE(q.date_of_service, 'yyyyMMdd') < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR TO_DATE(q.date_of_service, 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE) THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT (SUBSTR(q.date_of_service, 1, 4), '-', SUBSTR(q.date_of_service, 5, 2))
    END                                                                     AS part_best_date
FROM quest_labtest_base q
    LEFT JOIN matching_payload mp ON
        q.accn_id = mp.claimid AND q.hv_join_key = mp.hvJoinKey
    LEFT JOIN augmented_with_prov_attrs_mp prov_mp ON q.accn_id = prov_mp.claimId
WHERE
    q.diagnosis_code is null
