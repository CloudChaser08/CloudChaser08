-- diagnosis
SELECT replace(diag.problem_icd,'.','') as diagnosis_code,
    mp.hvid
FROM f_diagnosis diag
    LEFT JOIN matching_payload mp on diag.patient_key = mp.personid

-- drug
SELECT drug.ndc,
    mp.hvid
FROM f_medication med 
    INNER JOIN d_drug drug on med.drug_key = drug.drug_key
    LEFT JOIN matching_payload mp on med.patient_key = mp.personid 

-- procedure
SELECT cpt.cpt_code as cpt_code,
    mp.hvid
FROM f_procedure proc
    INNER JOIN d_cpt cpt on proc.cpt_key = cpt.cpt_key
    LEFT JOIN matching_payload mp on proc.patient_key = mp.personid
