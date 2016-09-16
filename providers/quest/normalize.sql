-- Load MERGED transaction data into table
-- COPY transactional_raw FROM :input_path CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
-- COPY matching_payload FROM :matching_path CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/payloadpaths.json';

INSERT INTO normalized_output (
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
    mp.age,                                                             --patient_age
    mp.yearOfBirth,                                                     --patient_year_of_birth
    mp.threeDigitZip,                                                   --patient_zip3
    mp.state,                                                           --patient_state
    CASE WHEN CHAR_LENGTH(LTRIM(q.date_of_service, '0')) >= 8 
    THEN SUBSTRING(q.date_of_service FROM 1 FOR 4) || '-' || SUBSTRING(q.date_of_service FROM 5 FOR 2) || '-' || SUBSTRING(q.date_of_service FROM 7 FOR 2) 
    ELSE NULL END,                                                      --date_service
    CASE WHEN CHAR_LENGTH(lTRIM(q.date_collected, '0')) >= 8 
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

