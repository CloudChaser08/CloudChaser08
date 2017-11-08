INSERT INTO lab_common_model
SELECT 
    NULL,                                -- record_id
    NULL,                                -- claim_id
    mp.hvid,                             -- hvid
    NULL,                                -- created
    '03',                                -- model_version
    NULL,                                -- data_set
    NULL,                                -- data_feed
    NULL,                                -- data_vendor
    NULL,                                -- source_version
    CASE
      WHEN UPPER(COALESCE(mp.gender, txn.patient_sex)) IN ('F', 'M')
      THEN UPPER(COALESCE(mp.gender, txn.patient_sex))
      ELSE 'U'
    END,                                 -- patient_gender
    NULL,                                -- patient_age
    mp.yearOfBirth,                      -- patient_year_of_birth
    mp.threeDigitZip,                    -- patient_zip3
    COALESCE(mp.state, txn.patient_st),  -- patient_state
    NULL,                                -- date_service
    EXTRACT_DATE(
        txn.pat_dos, '%Y%m%d'
        ),                               -- date_specimen
    NULL,                                -- date_report
    NULL,                                -- time_report
    txn.loinc_code,                      -- loinc_code
    NULL,                                -- lab_id
    NULL,                                -- test_id
    txn.test_number,                     -- test_number
    txn.test_ordered_code,               -- test_battery_local_id
    NULL,                                -- test_battery_std_id
    txn.test_ordered_name,               -- test_battery_name
    txn.perf_lab_code,                   -- test_ordered_local_id
    NULL,                                -- test_ordered_std_id
    txn.test_name,                       -- test_ordered_name
    NULL,                                -- result_id
    txn.result_dec,                      -- result
    txn.result_abbrev,                   -- result_name
    NULL,                                -- result_unit_of_measure
    NULL,                                -- result_desc
    NULL,                                -- result_comments
    txn.normal_dec_low,                  -- ref_range_low
    txn.normal_dec_high,                 -- ref_range_high
    NULL,                                -- ref_range_alpha
    CASE
      WHEN UPPER(txn.result_abn_code) IN ('Y', '1', 'P')
      THEN 'Y'
      WHEN UPPER(txn.result_abn_code) IN ('N', '0')
      THEN 'N'
    END,                                 -- abnormal_flag
    NULL,                                -- fasting_status
    NULL,                                -- diagnosis_code
    NULL,                                -- diagnosis_code_qual
    NULL,                                -- diagnosis_code_priority
    NULL,                                -- procedure_code
    NULL,                                -- procedure_code_qual
    NULL,                                -- lab_npi
    NULL,                                -- ordering_npi
    NULL,                                -- payer_id
    NULL,                                -- payer_id_qual
    NULL,                                -- payer_name
    NULL,                                -- payer_parent_name
    NULL,                                -- payer_org_name
    NULL,                                -- payer_plan_id
    NULL,                                -- payer_plan_name
    NULL,                                -- payer_type
    NULL,                                -- lab_other_id
    NULL,                                -- lab_other_qual
    NULL,                                -- ordering_other_id
    NULL,                                -- ordering_other_qual
    NULL,                                -- ordering_name
    NULL,                                -- ordering_market_type
    NULL,                                -- ordering_specialty
    NULL,                                -- ordering_vendor_id
    NULL,                                -- ordering_tax_id
    NULL,                                -- ordering_dea_id
    NULL,                                -- ordering_ssn
    NULL,                                -- ordering_state_license
    NULL,                                -- ordering_upin
    NULL,                                -- ordering_commercial_id
    NULL,                                -- ordering_address_1
    NULL,                                -- ordering_address_2
    NULL,                                -- ordering_city
    NULL,                                -- ordering_state
    NULL,                                -- ordering_zip
    NULL                                 -- logical_delete_reason
FROM transactions txn
    LEFT JOIN matching_payload mp ON txn.hvJoinKey = mp.hvJoinKey
WHERE txn.hvJoinKey IS NOT NULL
    AND 8 = LENGTH(txn.pat_dos)