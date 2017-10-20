INSERT INTO emr_common_model (
        hvid,
        source_version,
        claim_type,
        date_start,
        encounter_id,
        patient_year_of_birth,
        patient_zip,
        patient_gender,
        diagnosis_code,
        procedure_code,
        loinc_code,
        ndc_code
        )
SELECT DISTINCT
    COALESCE(payload.parentid, payload.hvid),
    '1',
    codes.claim_type,
    encounters.encounterDTTM,
    encounters.encounterid,
    payload.yearOfBirth,
    payload.threeDigitZip,
    CASE as_patients.gender 
    WHEN 'Male' THEN 'M'
    WHEN 'Female' THEN 'F'
    ELSE 'Unknown'
    END,
    TRIM(replace(codes.diagnosis_code,'.','')),
    codes.procedure_code,
    codes.loinc_code,
    codes.ndc_code
FROM transactional_encounters encounters
    INNER JOIN (
    -- diagnoses
    SELECT
        encounterid,
        gen2patientID,
        'ORDERS' as claim_type,
        billingicd10code AS diagnosis_code,
        null AS procedure_code,
        null as ndc_code,
        null as loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT DISTINCT
        encounterid,
        gen2patientID,
        'PROBLEMS' as claim_type,
        icd10 AS diagnosis_code,
        null AS procedure_code,
        null as ndc_code,
        null as loinc_code
    FROM transactional_problems
    WHERE errorflag = 'N'

    -- procedures
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'ORDERS' as claim_type,
        null AS diagnosis_code,
        cpt4 AS procedure_code,
        null as ndc_code,
        null as loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'ORDERS' as claim_type,
        null AS diagnosis_code,
        hcpcs AS procedure_code,
        null AS ndc_code,
        null AS loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'PROBLEMS' as claim_type,
        null AS diagnosis_code,
        cptcode AS procedure_code,
        null AS ndc_code,
        null AS loinc_code
    FROM transactional_problems
    WHERE errorflag='N'

    -- drugs
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'MEDICATIONS' as claim_type,
        null AS diagnosis_code,
        null AS procedure_code,
        ndc AS ndc_code,
        null AS loinc_code
    FROM transactional_medications
    WHERE errorflag='N'
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'VACCINES' as claim_type,
        null AS diagnosis_code,
        null AS procedure_code,
        ndc AS ndc_code,
        null AS loinc_code
    FROM transactional_vaccines

    -- lab
    UNION ALL
    SELECT
        encounterid,
        gen2patientID,
        'RESULTS' as claim_type,
        null AS diagnosis_code,
        null AS procedure_code,
        null AS ndc_code,
        loinc AS loinc_code
    FROM transactional_results 
        ) codes
    ON encounters.encounterid = codes.encounterid AND encounters.gen2patientID = codes.gen2patientID
    INNER JOIN transactional_patients as_patients ON encounters.gen2patientID = as_patients.gen2patientID
    INNER JOIN matching_payload payload ON encounters.gen2patientID = upper(payload.personid)
WHERE codes.diagnosis_code IS NOT NULL 
    OR codes.procedure_code IS NOT NULL
    OR codes.ndc_code IS NOT NULL
    OR codes.loinc_code IS NOT NULL
