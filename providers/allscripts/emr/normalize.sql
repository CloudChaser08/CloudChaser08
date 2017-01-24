INSERT INTO emr_common_model (
        hvid,
        source_version,
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
    as_patients.dobyear,
    payload.threeDigitZip,
    CASE as_patients.gender 
    WHEN 'Male' THEN 'M'
    WHEN 'Female' THEN 'F'
    ELSE 'Unknown'
    END,
    codes.diagnosis_code,
    codes.procedure_code,
    codes.loinc_code,
    codes.ndc_code
FROM transactional_encounters encounters
    INNER JOIN (
    -- diagnoses
    SELECT
        encounterid,
        genpatientID,
        gen2patientID,
        billingicd10code AS diagnosis_code,
        null AS procedure_code,
        null as ndc_code,
        null as loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT DISTINCT
        encounterid,
        genpatientID,
        gen2patientID,
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
        genpatientID,
        gen2patientID,
        null AS diagnosis_code,
        cpt4 AS procedure_code,
        null as ndc_code,
        null as loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT
        encounterid,
        genpatientID,
        gen2patientID,
        null AS diagnosis_code,
        hcpcs AS procedure_code,
        null as ndc_code,
        null as loinc_code
    FROM transactional_orders
    UNION ALL
    SELECT
        encounterid,
        genpatientID,
        gen2patientID,
        null AS diagnosis_code
        cptcode AS procedure_code,
        null AS ndc_code,
        null AS loinc_code
    FROM transactional_problems
    WHERE errorflag='N'

    -- drugs
    UNION ALL
    SELECT
        encounterid,
        genpatientID,
        gen2patientID,
        null AS diagnosis_code,
        null AS procedure_code,
        ndc AS ndc_code,
        null AS loinc_code
    FROM transactional_medications
    WHERE errorflag='N'
    UNION ALL
    SELECT
        encounterid,
        genpatientID,
        gen2patientID,
        null AS diagnosis_code,
        null AS procedure_code,
        ndc AS ndc_code,
        null AS loinc_code
    FROM transactional_vaccines

    -- lab
    UNION ALL
    SELECT
        encounterid,
        genpatientID,
        gen2patientID,
        null AS diagnosis_code,
        null AS procedure_code,
        null AS ndc_code,
        loinc AS code
    FROM transactional_results 
        ) codes
    ON encounters.encounterid = codes.encounterid
    AND encounters.genpatientID = codes.genpatientID
    AND encounters.gen2patientID = codes.gen2patientID
    INNER JOIN transactional_patients as_patients
    ON encounters.genpatientID = as_patients.genpatientID
    AND encounters.gen2patientID = as_patients.gen2patientID
    INNER JOIN matching_payload payload
    ON encounters.genpatientID = payload.claimid
    AND encounters.gen2patientID = payload.personid
