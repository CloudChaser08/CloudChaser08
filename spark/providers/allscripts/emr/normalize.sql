SELECT DISTINCT
    COALESCE(payload.parentid, payload.hvid)    AS hvid,
    '1'                                         AS source_version,
    codes.claim_type                            AS claim_type,
    encounters.encounterDTTM                    AS date_start,
    encounters.encounterid                      AS encounter_id,
    payload.yearOfBirth                         AS patient_year_of_birth,
    payload.threeDigitZip                       AS patient_zip,
    CASE
      WHEN as_patients.gender = 'Male' THEN 'M'
      WHEN as_patients.gender = 'Female' THEN 'F'
      ELSE 'Unknown'
    END                                         AS patient_gender,
    TRIM(replace(codes.diagnosis_code,'.',''))  AS diagnosis_code,
    codes.procedure_code                        AS procedure_code,
    codes.loinc_code                            AS loinc_code,
    codes.ndc_code                              AS ndc_code
FROM transactional_encounters encounters
    LEFT JOIN (
    -- diagnoses
    SELECT
        encounterid,
        gen2patientID,
        'ORDERS' AS claim_type,
        billingicd10code AS diagnosis_code,
        NULL AS procedure_code,
        NULL AS ndc_code,
        NULL AS loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT DISTINCT
        encounterid,
        gen2patientID,
        'PROBLEMS' AS claim_type,
        icd10 AS diagnosis_code,
        NULL AS procedure_code,
        NULL AS ndc_code,
        NULL AS loinc_code
    FROM transactional_problems
    WHERE errorflag = 'N'

    -- procedures
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'ORDERS' AS claim_type,
        NULL AS diagnosis_code,
        cpt4 AS procedure_code,
        NULL AS ndc_code,
        NULL AS loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'ORDERS' AS claim_type,
        NULL AS diagnosis_code,
        hcpcs AS procedure_code,
        NULL AS ndc_code,
        NULL AS loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'PROBLEMS' AS claim_type,
        NULL AS diagnosis_code,
        cptcode AS procedure_code,
        NULL AS ndc_code,
        NULL AS loinc_code
    FROM transactional_problems
    WHERE errorflag='N'

    -- drugs
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'MEDICATIONS' AS claim_type,
        NULL AS diagnosis_code,
        NULL AS procedure_code,
        ndc AS ndc_code,
        NULL AS loinc_code
    FROM transactional_medications
    WHERE errorflag='N'
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'VACCINES' AS claim_type,
        NULL AS diagnosis_code,
        NULL AS procedure_code,
        ndc AS ndc_code,
        NULL AS loinc_code
    FROM transactional_vaccines

    -- lab
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'RESULTS' AS claim_type,
        NULL AS diagnosis_code,
        NULL AS procedure_code,
        NULL AS ndc_code,
        loinc AS loinc_code
    FROM transactional_results
        ) codes
    ON encounters.encounterid = codes.encounterid AND encounters.gen2patientID = codes.gen2patientID
    INNER JOIN transactional_patients as_patients ON encounters.gen2patientID = as_patients.gen2patientID
    INNER JOIN matching_payload payload ON encounters.gen2patientID = UPPER(payload.personid)
