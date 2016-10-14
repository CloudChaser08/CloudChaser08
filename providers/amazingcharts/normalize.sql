INSERT INTO matching_payload (
        hvid, 
        state,
        gender,
        yearOfBirth
        )
SELECT COALESCE(pcm.parentId, mp.hvid)
    mp.state,
    mp.gender,
    mp.yearOfBirth
FROM matching_payload_broken mp
LEFT JOIN parent_child_map pcm ON mp.hvid = pcm.hvid
;

INSERT INTO full_transactional (
        hvid, 
        patient_gender, 
        patient_state, 
        patient_age, 
        drug, 
        diagnosis, 
        lab, 
        procedure
        )
SELECT mp.hvid,
    mp.gender,
    upper(mp.state),
    CASE 
    WHEN mp.yearOfBirth !~ '^[0-9]{4}$' THEN NULL
    WHEN (EXTRACT(year from current_date) - mp.yearOfBirth) >= 90 THEN '90'
    ELSE (EXTRACT(year from current_date) - mp.yearOfBirth)::varchar
    END,
    transactional.drug,
    transactional.diagnosis,
    transactional.lab,
    transactional.procedure
FROM (
        (
        SELECT patient_key, 
            replace(problem_icd,'.','') as diagnosis, 
            '' as drug, 
            '' as procedure,
            '' as lab
        FROM f_diagnosis diag
            ) 
    UNION ALL (
        SELECT med.patient_key, 
            '' as diagnosis, 
            drug.ndc as drug,
            '' as procedure,
            '' as lab
        FROM f_medication med 
            INNER JOIN d_drug drug on med.drug_key = drug.drug_key
            )
    UNION ALL (
        SELECT proc.patient_key,
            '' as diagnosis,
            '' as drug,
            cpt.cpt_code as procedure,
            '' as lab
        FROM f_procedure proc
            INNER JOIN d_cpt cpt on proc.cpt_key = cpt.cpt_key
            )
    UNION ALL (
        SELECT patient_key,
            '' as diagnosis,
            '' as drug,
            '' as procedure,
            loinc_test_code as lab
        FROM f_lab
            )
        ) transactional
    LEFT JOIN matching_payload mp ON transactional.patient_key = mp.personId
 ;
