-- Load MERGED transaction data into table
copy quest_raw from :input_path credentials :credentials BZIP2 EMPTYASNULL DELIMITER '|';

insert into normalized_output (
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
with numbers as (
select n::int
from
    (select 
        row_number() over (order by true) as n
    from quest_raw)
    cross join
    (select 
        max(length(diagnosis_code) - length(replace(diagnosis_code, '^', ''))) as max_num 
    from quest_raw)
where
    n <= max_num + 1
    )
SELECT accn_id,                                                             --claim_id
    '',                                                                     --hvid
    '',                                                                     --patient_gender
    '',                                                                     --patient_age
    '',                                                                     --patient_year_of_birth
    '',                                                                     --patient_zip3
    '',                                                                     --patient_state
    CASE WHEN char_length(ltrim(date_of_service, '0')) >= 8 
    THEN substring(date_of_service from 1 for 4) || '-' || substring(date_of_service from 5 for 2) || '-' || substring(date_of_service from 7 for 2) 
    ELSE NULL END,                                                          --date_service
    CASE WHEN char_length(ltrim(date_collected, '0')) >= 8 
    THEN substring(date_collected from 1 for 4) || '-' || substring(date_collected from 5 for 2) || '-' || substring(date_collected from 7 for 2) 
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
    split_part(upper(trim(replace(diagnosis_code,'.',''))),'^',numbers.n),  --diagnosis_code
    icd_codeset_ind                                                         --diagnosis_code_qual
FROM quest_raw
cross join numbers
WHERE split_part(trim(diagnosis_code),'^',numbers.n) is not null
    AND split_part(trim(diagnosis_code),'^',numbers.n) != '' 
    ;

