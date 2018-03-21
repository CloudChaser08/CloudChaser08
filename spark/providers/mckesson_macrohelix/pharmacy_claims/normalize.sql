INSERT INTO pharmacyclaims_common_model
SELECT DISTINCT
        t.row_id                                                                               AS claim_id,
        p.hvid                                                                                 AS hvid,
        CASE
            WHEN UPPER(TRIM(COALESCE(p.gender, t.patient_gender, 'U'))) IN ('F', 'M', 'U')
                THEN UPPER(TRIM(COALESCE(p.gender, t.patient_gender, 'U')))
            ELSE 'U'
        END                                                                                    AS patient_gender,
        COALESCE(p.yearOfBirth, t.birth_date)                                                  AS patient_year_of_birth,
        SUBSTR(TRIM(COALESCE(p.threeDigitZip, t.patient_zip)), 1, 3)                           AS patient_zip3,
        TRIM(UPPER(COALESCE(p.state, '')))                                                     AS patient_state,
        extract_date(
            t.service_date,
            '%Y-%m-%d'
        )                                                                                      AS date_service,
        CASE
        /* service date is null or occurs after the input date */
        WHEN EXTRACT_DATE(t.service_date, '%Y-%m-%d', NULL, CAST({date_input} AS DATE)) IS NULL THEN NULL
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
        END                                                                                    AS diagnosis_code,
        TRIM(UPPER(t.jcode))                                                                   AS procedure_code,
        t.ndc                                                                                  AS ndc_code,
        t.quantity                                                                             AS dispensed_quantity,
        t.insurance                                                                            AS payer_name,
        CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(t.insurance_3, ''))) THEN 3
            WHEN 0 <> LENGTH(TRIM(COALESCE(t.insurance_2, ''))) THEN 2
            WHEN 0 <> LENGTH(TRIM(COALESCE(t.insurance, ''))) THEN 1
            ELSE 0
        END                                                                                    AS cob_count,
        t.gross_charge                                                                         AS submitted_gross_due,
        t.hospital_zip                                                                         AS pharmacy_postal_code
FROM mckesson_macrohelix_transactions t
LEFT OUTER JOIN matching_payload p ON t.hvJoinKey = p.hvJoinKey
CROSS JOIN exploder e

WHERE
-- include rows for non-null diagnoses
    ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
          t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
          t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
          t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
          t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n] IS NOT NULL
-- if all diagnoses are null, include one row w/ null diagnosis_code
-- this will only keep one b/c we are doing a SELECT DISTINCT
OR
(
    COALESCE(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
          t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
          t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
          t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
          t.dx_21, t.dx_22, t.dx_23, t.dx_24) IS NULL
)
;

