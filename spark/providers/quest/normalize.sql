INSERT INTO lab_common_model
SELECT
    NULL,                                                                            --record_id
    CONCAT(TRIM(q.accn_id), '_', q.dosid),                                           --claim_id
    COALESCE(mp.parentId,mp.hvid),                                                   --hvid
    {today},                                                                         --created
    '1',                                                                             --model_version
    {filename},                                                                      --data_set
    {feedname},                                                                      --data_feed
    {vendor},                                                                        --data_vendor
    '1',                                                                             --source_version
    mp.gender,                                                                       --patient_gender
    cast(cast(mp.age as int) as string),                                             --patient_age
    cast(cast(mp.yearOfBirth as int) as string),                                     --patient_year_of_birth
    mp.threeDigitZip,                                                                --patient_zip3
    mp.state,                                                                        --patient_state
    service.formatted,                                                               --date_service
    collected.formatted,                                                             --date_specimen
    NULL,                                                                            --date_report 
    NULL,                                                                            --time_report
    q.loinc_code,                                                                    --loinc_code
    q.lab_id,                                                                        --lab_id
    NULL,                                                                            --test_id
    NULL,                                                                            --test_number
    NULL,                                                                            --test_battery_local_id
    NULL,                                                                            --test_battery_std_id
    NULL,                                                                            --test_battery_name
    q.local_order_code,                                                              --test_ordered_local_id
    q.standard_order_code,                                                           --test_ordered_std_id
    q.order_name,                                                                    --test_ordered_name
    q.local_result_code,                                                             --result_id
    NULL,                                                                            --result
    q.result_name,                                                                   --result_name
    NULL,                                                                            --result_unit_of_measure
    NULL,                                                                            --result_desc
    NULL,                                                                            --result_comments
    NULL,                                                                            --ref_range_low
    NULL,                                                                            --ref_range_high
    NULL,                                                                            --ref_range_alpha
    NULL,                                                                            --abnormal_flag
    NULL,                                                                            --fasting_status
    SPLIT(UPPER(TRIM(REGEXP_REPLACE(q.diagnosis_code, '[.|,| ]', ''))),'\\^')[n.n],  --diagnosis_code
    CASE q.icd_codeset_ind
    WHEN '9' THEN '01' WHEN '0' THEN '02'
    END,                                                                             --diagnosis_code_qual
    NULL,                                                                            --diagnosis_code_priority  
    NULL,                                                                            --procedure_code          
    NULL,                                                                            --procedure_code_qual     
    NULL,                                                                            --lab_npi                 
    NULL,                                                                            --ordering_npi            
    NULL,                                                                            --payer_id                
    NULL,                                                                            --payer_id_qual           
    NULL,                                                                            --payer_name              
    NULL,                                                                            --payer_parent_name       
    NULL,                                                                            --payer_org_name          
    NULL,                                                                            --payer_plan_id           
    NULL,                                                                            --payer_plan_name         
    NULL,                                                                            --payer_type              
    NULL,                                                                            --lab_other_id            
    NULL,                                                                            --lab_other_qual          
    NULL,                                                                            --ordering_other_id       
    NULL,                                                                            --ordering_other_qual     
    NULL,                                                                            --ordering_market_type    
    NULL                                                                             --ordering_specialty
FROM transactional_raw q
    LEFT JOIN matching_payload mp ON {join}
    LEFT JOIN dates service ON q.date_of_service = service.date
    LEFT JOIN dates collected ON q.date_collected = collected.date
    CROSS JOIN diagnosis_exploder n
WHERE (
        SPLIT(TRIM(q.diagnosis_code),'\\^')[n.n] IS NOT NULL
        AND SPLIT(TRIM(q.diagnosis_code),'\\^')[n.n] != ''
        )
    OR q.diagnosis_code IS NULL
    OR q.diagnosis_code = ''
    ;
