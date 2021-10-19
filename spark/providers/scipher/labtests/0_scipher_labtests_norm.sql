SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.sample_token                                                                        AS claim_id,
    COALESCE(pay.hvid, CONCAT('239_', pay.claimid))                                         AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
    '239'                                                                                   AS data_feed,
    '633'                                                                                   AS data_vendor,
    /* patient_gender */
    CASE
        WHEN pay.gender is NULL                     THEN NULL
        WHEN UPPER(pay.gender) IN ('F', 'M', 'U')   THEN UPPER(pay.gender)
        ELSE 'U'
    END                                                                                     AS patient_gender,
    /* patient_year_of_birth */
    CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(
                COALESCE
                    (
                        EXTRACT_DATE(txn.sample_collection_date, '%Y-%m-%d'),
                        '{VDR_FILE_DT}',
                        EXTRACT_DATE('1900-01-01',               '%Y-%m-%d')
                    ) AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    /* patient_zip3 */
    MASK_ZIP_CODE
        (
            SUBSTR(pay.threedigitzip, 1, 3)
        )                                                                                   AS patient_zip3,
    /* patient_state */
    VALIDATE_STATE_CODE
        (
            UPPER(COALESCE(pay.state, ''))
        )                                                                                   AS patient_state,
    /* date_specimen */
     CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.sample_collection_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'                       AS DATE),
            CAST('{VDR_FILE_DT}'                                       AS DATE)
        )                                                                                   AS date_specimen,
    /* loinc_code  */
    CASE

        WHEN result_pivot.n = 3     THEN CLEAN_UP_LOINC_CODE(txn.crp_loinc)
        WHEN result_pivot.n = 4     THEN CLEAN_UP_LOINC_CODE(txn.anti_ccp_loinc)
        ELSE NULL
    END                                                                                     AS loinc_code,
    /* lab_id   */
    txn.performing_lab                                                                      AS lab_id,
    /* result   */
    ARRAY (
                txn.registry,
                txn.disease_activity,
                txn.tnf_exposure,
                txn.crp,
                txn.anti_ccp,
                CLEAN_UP_VITAL_SIGN('BMI', txn.bmi, 'INDEX', pay.gender, NULL, pay.yearofbirth, CAST(txn.sample_collection_date AS DATE), NULL),
                txn.non_response_signal,
                txn.prism_ra_score,
                txn.prism_ra_raw_score
            )[result_pivot.n]                                                               AS result,
    /*  result_name     */
    CASE
        WHEN result_pivot.n = 0     THEN UPPER('registry')
        WHEN result_pivot.n = 1     THEN UPPER('disease_activity')
        WHEN result_pivot.n = 2     THEN UPPER('tnf_exposure')
        WHEN result_pivot.n = 3     THEN UPPER('crp')
        WHEN result_pivot.n = 4     THEN UPPER('anti_ccp')
        WHEN result_pivot.n = 5     THEN UPPER('bmi')
        WHEN result_pivot.n = 6     THEN UPPER('non_response_signal')
        WHEN result_pivot.n = 7     THEN UPPER('prism_ra_score')
        WHEN result_pivot.n = 8     THEN UPPER('prism_ra_raw_score')
        ELSE NULL
    END                                                                                  AS result_name,
    /*  result_desc     */
    CASE
        WHEN result_pivot.n = 3     THEN CONCAT(txn.crp_less_greater_than_threshold,      " THRESHOLD")
        WHEN result_pivot.n = 4     THEN CONCAT(txn.anti_ccp_less_greater_than_threshold, " THRESHOLD")
        ELSE NULL
    END                                                                                 AS result_desc,
    /*  ordering_npi    */
    CLEAN_UP_NPI_CODE(txn.npi )                                                         AS ordering_npi,
    /*  lab_other_id    */
    txn.scipher_practice_account_number                                                 AS lab_other_id,
    /*  lab_other_qual  */
    CASE
        WHEN txn.scipher_practice_account_number is NOT NULL
            THEN    'SCIPHER_PRACTICE_ACCOUNT_NUMBER'
        ELSE NULL
    END                                                                                 AS lab_other_qual,
    /*  part_provider   */
    'scipher'                                                                           AS part_provider,
    /*  part_best_date  */
    CASE
        WHEN CAP_DATE
             (
                CAST(EXTRACT_DATE(txn.sample_collection_date, '%Y-%m-%d')                           AS DATE),
                CAST(COALESCE('{EARLIEST_SERVICE_DATE}', '{AVAILABLE_START_DATE}')  AS DATE),
                CAST('{VDR_FILE_DT}'                                                                 AS DATE)
             ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                    SUBSTR(EXTRACT_DATE(txn.sample_collection_date, '%Y-%m-%d'), 1, 8), '01'
                )
    END                                                                                 AS part_best_date
FROM txn txn
LEFT OUTER JOIN matching_payload pay ON txn.hvjoinkey = pay.hvjoinkey
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8)) AS n) result_pivot
WHERE
-- Remove header records
   UPPER(txn.sample_token) <> 'SAMPLE_TOKEN'
--limit 10
