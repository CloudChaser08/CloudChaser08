INSERT INTO full_transacational (
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
    mp.state,
    mp.yearOfBirth,
    t.drug,
    t.diagnosis,
    t.lab,
    t.procedure
FROM (
        (
        SELECT patient_key, 
            replace(problem_icd,'.','') as diagnosis, 
            "" as drug, 
            "" as procedure,
            "" as lab
        FROM f_diagnosis diag
            ) 
    UNION ALL (
        SELECT med.patient_key, 
            "" as diagnosis, 
            drug.ndc as drug,
            "" as procedure,
            "" as lab
        FROM f_medication med 
            INNER JOIN d_drug drug on med.drug_key = drug.drug_key
            )
    UNION ALL (
        SELECT proc.patient_key,
            "" as diagnosis,
            "" as drug,
            cpt.cpt_code as procedure,
            "" as lab
        FROM f_procedure proc
            INNER JOIN d_cpt cpt on proc.cpt_key = cpt.cpt_key
            )
    UNION ALL (
        SELECT patient_key,
            "" as diagnosis,
            "" as drug,
            "" as procedure,
            loinc_test_code as lab
        FROM f_lab
            )
        ) transactional t
    LEFT JOIN matching_payload mp ON t.patient_key = mp.personId
