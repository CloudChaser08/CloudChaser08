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
        lab_id,
        test_id,
        test_number,
        test_battery_local_id,
        test_battery_std_id,
        test_battery_name,
        test_ordered_local_id,
        test_ordered_std_id,
        test_ordered_name,
        result,
        result_name,
        result_unit_of_measure,
        result_desc,
        result_comments,
        diagnosis_code,
        diagnosis_code_qual
        )
SELECT TRIM(q.accn_id),                                                 --claim_id
    mp.hvid,                                                            --hvid
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
    service.formatted,                                                                --date_service
    collected.formatted,                                                                --date_specimen
    q.loinc_code,                                                       --loinc_code
    null,                                                               --lab_id ** we don't have this yet
    null,                                                               --test_id
    null,                                                               --test_number
    null,                                                               --test_battery_local_id
    null,                                                               --test_battery_std_id
    null,                                                               --test_battery_name
    null,                                                               --test_ordered_local_id
    null,                                                               --test_ordered_std_id
    null,                                                               --test_ordered_name
    null,                                                               --result
    null,                                                               --result_name
    null,                                                               --result_unit_of_measure
    null,                                                               --result_desc
    null,                                                               --result_comments
    split_part(UPPER(TRIM(replace(q.diagnosis_code,'.',''))),'^',n.n),  --diagnosis_code
    q.icd_codeset_ind                                                   --diagnosis_code_qual
FROM transactional_raw q
    LEFT JOIN matching_payload mp on q.accn_id = mp.claimid
    LEFT JOIN dates service ON q.date_of_service = service.date
    LEFT JOIN dates collected ON q.date_collected = collected.date
    CROSS JOIN diagnosis_exploder n
WHERE split_part(TRIM(q.diagnosis_code),'^',n.n) IS NOT NULL
    AND split_part(TRIM(q.diagnosis_code),'^',n.n) != '' 
    ;

