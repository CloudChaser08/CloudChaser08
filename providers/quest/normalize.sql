-- Load MERGED transaction data into table
COPY quest_raw FROM :input_path credentials :credentials BZIP2 EMPTYASNULL DELIMITER '|';

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
    FROM quest_raw 
        )
    CROSS JOIN (
    SELECT
        MAX(LENGTH(diagnosis_code) - LENGTH(replace(diagnosis_code, '^', ''))) AS max_num 
    FROM quest_raw 
        )
WHERE
    n <= max_num + 1
    )
SELECT accn_id,                                                             --claim_id
    '',                                                                     --hvid
    '',                                                                     --patient_gender
    '',                                                                     --patient_age
    '',                                                                     --patient_year_of_birth
    '',                                                                     --patient_zip3
    '',                                                                     --patient_state
    CASE WHEN CHAR_LENGTH(lTRIM(date_of_service, '0')) >= 8 
    THEN SUBSTRING(date_of_service FROM 1 FOR 4) || '-' || SUBSTRING(date_of_service FROM 5 FOR 2) || '-' || SUBSTRING(date_of_service FROM 7 FOR 2) 
    ELSE NULL END,                                                          --date_service
    CASE WHEN CHAR_LENGTH(lTRIM(date_collected, '0')) >= 8 
    THEN SUBSTRING(date_collected FROM 1 FOR 4) || '-' || SUBSTRING(date_collected FROM 5 FOR 2) || '-' || SUBSTRING(date_collected FROM 7 FOR 2) 
    ELSE NULL END,                                                          --date_specimen
    loinc_code,                                                             --loinc_code
    '',                                                                     --lab_id
    '',                                                                     --test_id
    '',                                                                     --test_number
    '',                                                                     --test_battery_local_id
    '',                                                                     --test_battery_std_id
    '',                                                                     --test_battery_name
    '',                                                                     --test_ordered_local_id
    '',                                                                     --test_ordered_std_id
    '',                                                                     --test_ordered_name
    '',                                                                     --result
    '',                                                                     --result_name
    '',                                                                     --result_unit_of_measure
    '',                                                                     --result_desc
    '',                                                                     --result_comments
    split_part(UPPER(TRIM(replace(diagnosis_code,'.',''))),'^',numbers.n),  --diagnosis_code
    icd_codeset_ind                                                         --diagnosis_code_qual
FROM quest_raw
CROSS JOIN numbers
WHERE split_part(TRIM(diagnosis_code),'^',numbers.n) IS NOT NULL
    AND split_part(TRIM(diagnosis_code),'^',numbers.n) != '' 
    ;

