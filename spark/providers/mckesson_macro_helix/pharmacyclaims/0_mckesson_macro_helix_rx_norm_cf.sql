SELECT DISTINCT
        t.row_id                                                                                AS claim_id,
        p.hvid                                                                                  AS hvid,
        CURRENT_DATE()                                                                          AS created,
        '06'                                                                                    AS model_version,
        SPLIT(t.input_file_name, '/')[SIZE(SPLIT(t.input_file_name, '/')) - 1]
                                                                                                AS data_set,
        '51'                                                                                    AS data_feed,
        '86'                                                                                    AS data_vendor,
        CLEAN_UP_GENDER
            (
                CASE
                    WHEN UPPER(TRIM(COALESCE(p.gender, t.patient_gender, 'U'))) IN ('F', 'M', 'U')
                        THEN UPPER(TRIM(COALESCE(p.gender, t.patient_gender, 'U')))
                    ELSE 'U'
                END
            )                                                                                   AS patient_gender,
        CAP_YEAR_OF_BIRTH
            (
                NULL,
                CAST(EXTRACT_DATE(t.service_date, '%Y-%m-%d') AS DATE),
                COALESCE(p.yearOfBirth, t.birth_date)
            )                                                                                   AS patient_year_of_birth,
        MASK_ZIP_CODE
            (
                SUBSTR(TRIM(COALESCE(p.threeDigitZip, t.patient_zip)), 1, 3)
            )                                                                                   AS patient_zip3,
        VALIDATE_STATE_CODE(TRIM(UPPER(COALESCE(p.state, ''))))                                 AS patient_state,
        EXTRACT_DATE(
            t.service_date, '%Y-%m-%d', CAST('{EARLIEST_SERVICE_DATE}' AS DATE), CAST('{VDR_FILE_DT}' AS DATE)
            )                                                                                   AS date_service,
        CLEAN_UP_DIAGNOSIS_CODE
            (
                CASE
                    /* service date is null or occurs after the input date */
                    WHEN EXTRACT_DATE(t.service_date, '%Y-%m-%d', NULL, CAST('{VDR_FILE_DT}' AS DATE)) IS NULL THEN NULL
                    WHEN SUBSTRING(UPPER(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                            t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                            t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                            t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                            t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n]), 1, 1) = 'A'
                    AND SUBSTRING(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                            t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                            t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                            t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                            t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n], 5, 1) = '.'
                    THEN SUBSTRING(UPPER(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                            t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                            t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                            t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                            t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n]), 2, 10)
                    ELSE UPPER(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                            t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                            t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                            t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                            t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n])
                END,
                NULL,
                CAST(EXTRACT_DATE(t.service_date, '%Y-%m-%d') AS DATE)
            )                                                                                   AS diagnosis_code,
        CLEAN_UP_PROCEDURE_CODE(TRIM(UPPER(t.jcode)))                                           AS procedure_code,
        CLEAN_UP_NDC_CODE(t.ndc)                                                                AS ndc_code,
        t.quantity                                                                              AS dispensed_quantity,
        t.insurance                                                                             AS payer_name,
        CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(t.insurance_3, ''))) THEN 3
            WHEN 0 <> LENGTH(TRIM(COALESCE(t.insurance_2, ''))) THEN 2
            WHEN 0 <> LENGTH(TRIM(COALESCE(t.insurance, ''))) THEN 1
            ELSE 0
        END                                                                                     AS cob_count,
        t.gross_charge                                                                          AS submitted_gross_due,
        t.hospital_zip                                                                          AS pharmacy_postal_code,
        'mckesson_macro_helix'                                                                  AS part_provider,

        /* part_best_date */
        CASE
            WHEN 0 = LENGTH(TRIM(COALESCE
                                    (
                                        CAP_DATE
                                            (
                                                CAST(EXTRACT_DATE(t.service_date, '%Y-%m-%d') AS DATE),
                                                COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE)),
                                                CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                            ),
                                        ''
                                    )))
                THEN '0_PREDATES_HVM_HISTORY'
            ELSE SUBSTR(CAST(EXTRACT_DATE(t.service_date, '%Y-%m-%d') AS DATE), 1, 7)
        END                                                                                     AS part_best_date
FROM txn t
LEFT OUTER JOIN matching_payload p ON t.hvJoinKey = p.hvJoinKey
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                21, 22, 23)) AS n) e
WHERE
-- include rows for non-null diagnoses
    ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
          t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
          t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
          t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
          t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n] IS NOT NULL
OR
-- if all diagnoses are null, include one row w/ null diagnosis_code
-- this will only keep one b/c we are doing a SELECT DISTINCT
(
    COALESCE(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
          t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
          t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
          t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
          t.dx_21, t.dx_22, t.dx_23, t.dx_24) IS NULL
)

