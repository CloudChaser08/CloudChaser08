INSERT INTO lab_common_model (
        claim_id,
        hvid,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        date_service,
        date_specimen,
        loinc_code,
        test_ordered_local_id,
        test_ordered_std_id,
        test_ordered_name,
        result_name,
        diagnosis_code,
        diagnosis_code_qual
        )
SELECT TRIM(q.accn_id),                                                 --claim_id
    COALESCE(mp.parentId,mp.hvid),                                      --hvid
    mp.gender,                                                          --patient_gender
    CASE 
    WHEN mp.age !~ '^[0-9]+$' THEN NULL
    WHEN mp.age::int >= 85 THEN '90' 
    ELSE mp.age
    END,                                                                --patient_age
    CASE 
    WHEN mp.yearOfBirth !~ '^[0-9]+$' THEN NULL
    WHEN mp.yearOfBirth::int NOT BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE) THEN NULL
    ELSE mp.yearOfBirth
    END,                                                                --patient_year_of_birth
    mp.threeDigitZip,                                                   --patient_zip3
    mp.state,                                                           --patient_state
    service.formatted,                                                  --date_service
    collected.formatted,                                                --date_specimen
    q.loinc_code,                                                       --loinc_code
    q.local_order_code,                                                 --test_ordered_local_id
    q.standard_order_code,                                              --test_ordered_std_id
    q.order_name,                                                       --test_ordered_name
    q.result_name,                                                      --result_name
    split_part(UPPER(TRIM(replace(q.diagnosis_code,'.',''))),'^',n.n),  --diagnosis_code
    CASE q.icd_codeset_ind
    WHEN '9' THEN '01' WHEN '0' THEN '02'
    END                                                                 --diagnosis_code_qual
FROM transactional_raw q
    LEFT JOIN matching_payload mp ON q.accn_id = mp.claimid AND mp.hvJoinKey = q.hv_join_key
    LEFT JOIN dates service ON q.date_of_service = service.date
    LEFT JOIN dates collected ON q.date_collected = collected.date
    CROSS JOIN diagnosis_exploder n
WHERE split_part(TRIM(q.diagnosis_code),'^',n.n) IS NOT NULL
    AND split_part(TRIM(q.diagnosis_code),'^',n.n) != '' 
    ;
