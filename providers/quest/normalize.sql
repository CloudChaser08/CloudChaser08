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
WITH numbers AS (
    SELECT n::int
    FROM (
        SELECT
            row_number() over (ORDER BY true) AS n
        FROM transactional_raw
            )
        CROSS JOIN (
        SELECT
            MAX(LENGTH(diagnosis_code) - LENGTH(replace(diagnosis_code, '^', ''))) AS max_num 
        FROM transactional_raw
            )
    WHERE
        n <= max_num + 1
    )
SELECT TRIM(q.accn_id),                                                 --claim_id
    mp.hvid,                                                            --hvid
    mp.gender,                                                          --patient_gender
    CASE 
    WHEN mp.age~E'^\\d+$' THEN '0' 
    WHEN mp.age::int >= 85 THEN '90' 
    ELSE mp.age
    END,                                                                --patient_age
    mp.yearOfBirth,                                                     --patient_year_of_birth
    mp.threeDigitZip,                                                   --patient_zip3
    mp.state,                                                           --patient_state
    CASE WHEN (LENGTH(LTRIM(q.date_of_service, '0')) == 8 AND is_date_valid(LTRIM(q.date_of_service,'0')))
    THEN SUBSTRING(q.date_of_service FROM 1 FOR 4) || '-' || SUBSTRING(q.date_of_service FROM 5 FOR 2) || '-' || SUBSTRING(q.date_of_service FROM 7 FOR 2) 
    ELSE NULL END,                                                      --date_service
    CASE WHEN (LENGTH(LTRIM(q.date_collected, '0')) == 8 AND is_date_valid(LTRIM(q.date_collected,'0')))
    THEN SUBSTRING(q.date_collected FROM 1 FOR 4) || '-' || SUBSTRING(q.date_collected FROM 5 FOR 2) || '-' || SUBSTRING(q.date_collected FROM 7 FOR 2) 
    ELSE NULL END,                                                      --date_specimen
    q.loinc_code,                                                       --loinc_code
    '',                                                                 --lab_id ** we don't have this yet
    '',                                                                 --test_id
    '',                                                                 --test_number
    '',                                                                 --test_battery_local_id
    '',                                                                 --test_battery_std_id
    '',                                                                 --test_battery_name
    '',                                                                 --test_ordered_local_id
    '',                                                                 --test_ordered_std_id
    '',                                                                 --test_ordered_name
    '',                                                                 --result
    '',                                                                 --result_name
    '',                                                                 --result_unit_of_measure
    '',                                                                 --result_desc
    '',                                                                 --result_comments
    split_part(UPPER(TRIM(replace(q.diagnosis_code,'.',''))),'^',n.n),  --diagnosis_code
    q.icd_codeset_ind                                                   --diagnosis_code_qual
FROM transactional_raw q
    LEFT JOIN matching_payload mp on TRIM(q.accn_id) = TRIM(mp.claimid)
    CROSS JOIN numbers n
WHERE split_part(TRIM(q.diagnosis_code),'^',n.n) IS NOT NULL
    AND split_part(TRIM(q.diagnosis_code),'^',n.n) != '' 
    ;

