INSERT INTO normalized_data
SELECT
    mp.hvid,
    mp.gender,
    UPPER(mp.state),
    SUBSTRING(CURRENT_DATE(), 0, 4) - CAST(mp.yearOfBirth AS int),
    transactional.drug,
    clean_up_diagnosis_code(transactional.diagnosis, '02', NULL),
    transactional.lab,
    transactional.procedure,
    enc.encounter_date
FROM (
        (
        SELECT patient_key, 
            problem_icd as diagnosis, 
            '' as drug, 
            '' as procedure,
            '' as lab
        FROM f_diagnosis diag
            )
    UNION (
        SELECT med.patient_key, 
            '' as diagnosis, 
            ndc.ndc as drug,
            '' as procedure,
            '' as lab
        FROM f_medication med 
            INNER JOIN d_drug drug on med.drug_key = drug.drug_key
            INNER JOIN d_multum_to_ndc ndc on drug.drug_id = ndc.multum_id
            )
    UNION (
        SELECT proc.patient_key,
            '' as diagnosis,
            '' as drug,
            cpt.cpt_code as procedure,
            '' as lab
        FROM f_procedure proc
            INNER JOIN d_cpt cpt on proc.cpt_key = cpt.cpt_key
            )
    UNION (
        SELECT patient_key,
            '' as diagnosis,
            '' as drug,
            '' as procedure,
            loinc_test_code as lab
        FROM f_lab
            )
        ) transactional
    LEFT JOIN matching_payload mp ON transactional.patient_key = mp.personId
    LEFT JOIN (
        SELECT patient_key, max(encounter_date) as encounter_date
        FROM f_encounter
        GROUP BY patient_key
            ) enc ON transactional.patient_key = enc.patient_key
    ;
