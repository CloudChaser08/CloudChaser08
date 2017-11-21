-- insert all rows with diagnoses
INSERT INTO lab_common_model
SELECT
    NULL,                                   -- record_id
    CONCAT(q.accn_id, '_', q.dosid),        -- claim_id
    COALESCE(prov_mp.hvid, mp.hvid),        -- hvid
    NULL,                                   -- created
    '3',                                    -- model_version
    NULL,                                   -- data_set
    NULL,                                   -- data_feed
    NULL,                                   -- data_vendor
    '1',                                    -- source_version
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.gender, mp.gender)
    ELSE mp.gender
    END,                                    -- patient_gender
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.age, mp.age)
    ELSE mp.age
    END,                                    -- patient_age
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.yearOfBirth, mp.yearOfBirth)
    ELSE mp.yearOfBirth
    END,                                    -- patient_year_of_birth
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.threeDigitZip, mp.threeDigitZip)
    ELSE mp.threeDigitZip
    END,                                    -- patient_zip3
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.state, mp.state)
    ELSE mp.state
    END,                                    -- patient_state
    extract_date(
        q.date_of_service, '%Y%m%d'
        ),                                  -- date_service
    extract_date(
        q.date_collected, '%Y%m%d'
        ),                                  -- date_specimen
    NULL,                                   -- date_report
    NULL,                                   -- time_report
    q.loinc_code,                           -- loinc_code
    q.lab_id,                               -- lab_id
    NULL,                                   -- test_id
    NULL,                                   -- test_number
    NULL,                                   -- test_battery_local_id
    NULL,                                   -- test_battery_std_id
    NULL,                                   -- test_battery_name
    q.local_order_code,                     -- test_ordered_local_id
    q.standard_order_code,                  -- test_ordered_std_id
    q.order_name,                           -- test_ordered_name
    q.local_result_code,                    -- result_id
    NULL,                                   -- result
    q.result_name,                          -- result_name
    NULL,                                   -- result_unit_of_measure
    NULL,                                   -- result_desc
    NULL,                                   -- result_comments
    NULL,                                   -- ref_range_low
    NULL,                                   -- ref_range_high
    NULL,                                   -- ref_range_alpha
    NULL,                                   -- abnormal_flag
    NULL,                                   -- fasting_status
    SPLIT(q.diagnosis_code, '\\^')[n.n],    -- diagnosis_code
    CASE q.icd_codeset_ind
    WHEN '9' THEN '01' WHEN '0' THEN '02'
    END,                                    -- diagnosis_code_qual
    NULL,                                   -- diagnosis_code_priority
    NULL,                                   -- procedure_code
    NULL,                                   -- procedure_code_qual
    NULL,                                   -- lab_npi
    q.npi,                                  -- ordering_npi
    NULL,                                   -- payer_id
    NULL,                                   -- payer_id_qual
    NULL,                                   -- payer_name
    NULL,                                   -- payer_parent_name
    NULL,                                   -- payer_org_name
    NULL,                                   -- payer_plan_id
    NULL,                                   -- payer_plan_name
    NULL,                                   -- payer_type
    NULL,                                   -- lab_other_id
    NULL,                                   -- lab_other_qual
    NULL,                                   -- ordering_other_id
    NULL,                                   -- ordering_other_qual
    NULL,                                   -- ordering_name
    NULL,                                   -- ordering_market_type
    NULL,                                   -- ordering_specialty
    NULL,                                   -- ordering_vendor_id
    NULL,                                   -- ordering_tax_id
    NULL,                                   -- ordering_dea_id
    NULL,                                   -- ordering_ssn
    NULL,                                   -- ordering_state_license
    NULL,                                   -- ordering_upin
    NULL,                                   -- ordering_commercial_id
    NULL,                                   -- ordering_address_1
    NULL,                                   -- ordering_address_2
    NULL,                                   -- ordering_city
    NULL,                                   -- ordering_state
    q.acct_zip,                             -- ordering_zip
    NULL                                    -- logical_delete_reason
FROM transactional_raw q
    LEFT JOIN original_mp mp ON {join}
    LEFT JOIN augmented_with_prov_attrs_mp prov_mp ON q.accn_id = prov_mp.claimId
    CROSS JOIN diagnosis_exploder n

-- implicit here is that q.diagnosis_code itself is not null or blank
WHERE SPLIT(TRIM(q.diagnosis_code),'\\^')[n.n] IS NOT NULL
    AND SPLIT(TRIM(q.diagnosis_code),'\\^')[n.n] != ''
    ;

-- insert all rows without diagnoses
INSERT INTO lab_common_model
SELECT
    NULL,                                   -- record_id
    CONCAT(q.accn_id, '_', q.dosid),        -- claim_id
    COALESCE(prov_mp.hvid, mp.hvid),        -- hvid
    NULL,                                   -- created
    '3',                                    -- model_version
    NULL,                                   -- data_set
    NULL,                                   -- data_feed
    NULL,                                   -- data_vendor
    '1',                                    -- source_version
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.gender, mp.gender)
    ELSE mp.gender
    END,                                    -- patient_gender
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.age, mp.age)
    ELSE mp.age
    END,                                    -- patient_age
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.yearOfBirth, mp.yearOfBirth)
    ELSE mp.yearOfBirth
    END,                                    -- patient_year_of_birth
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.threeDigitZip, mp.threeDigitZip)
    ELSE mp.threeDigitZip
    END,                                    -- patient_zip3
    CASE
    WHEN prov_mp.hvid IS NOT NULL
    THEN COALESCE(prov_mp.state, mp.state)
    ELSE mp.state
    END,                                    -- patient_state
    extract_date(
        q.date_of_service, '%Y%m%d'
        ),                                  -- date_service
    extract_date(
        q.date_collected, '%Y%m%d'
        ),                                  -- date_specimen
    NULL,                                   -- date_report
    NULL,                                   -- time_report
    q.loinc_code,                           -- loinc_code
    q.lab_id,                               -- lab_id
    NULL,                                   -- test_id
    NULL,                                   -- test_number
    NULL,                                   -- test_battery_local_id
    NULL,                                   -- test_battery_std_id
    NULL,                                   -- test_battery_name
    q.local_order_code,                     -- test_ordered_local_id
    q.standard_order_code,                  -- test_ordered_std_id
    q.order_name,                           -- test_ordered_name
    q.local_result_code,                    -- result_id
    NULL,                                   -- result
    q.result_name,                          -- result_name
    NULL,                                   -- result_unit_of_measure
    NULL,                                   -- result_desc
    NULL,                                   -- result_comments
    NULL,                                   -- ref_range_low
    NULL,                                   -- ref_range_high
    NULL,                                   -- ref_range_alpha
    NULL,                                   -- abnormal_flag
    NULL,                                   -- fasting_status
    NULL,                                   -- diagnosis_code
    NULL,                                   -- diagnosis_code_qual
    NULL,                                   -- diagnosis_code_priority
    NULL,                                   -- procedure_code
    NULL,                                   -- procedure_code_qual
    NULL,                                   -- lab_npi
    q.npi,                                  -- ordering_npi
    NULL,                                   -- payer_id
    NULL,                                   -- payer_id_qual
    NULL,                                   -- payer_name
    NULL,                                   -- payer_parent_name
    NULL,                                   -- payer_org_name
    NULL,                                   -- payer_plan_id
    NULL,                                   -- payer_plan_name
    NULL,                                   -- payer_type
    NULL,                                   -- lab_other_id
    NULL,                                   -- lab_other_qual
    NULL,                                   -- ordering_other_id
    NULL,                                   -- ordering_other_qual
    NULL,                                   -- ordering_name
    NULL,                                   -- ordering_market_type
    NULL,                                   -- ordering_specialty
    NULL,                                   -- ordering_vendor_id
    NULL,                                   -- ordering_tax_id
    NULL,                                   -- ordering_dea_id
    NULL,                                   -- ordering_ssn
    NULL,                                   -- ordering_state_license
    NULL,                                   -- ordering_upin
    NULL,                                   -- ordering_commercial_id
    NULL,                                   -- ordering_address_1
    NULL,                                   -- ordering_address_2
    NULL,                                   -- ordering_city
    NULL,                                   -- ordering_state
    q.acct_zip,                             -- ordering_zip
    NULL                                    -- logical_delete_reason
FROM transactional_raw q
    LEFT JOIN original_mp mp ON {join}
    LEFT JOIN augmented_with_prov_attrs_mp prov_mp ON q.accn_id = prov_mp.claimId
WHERE q.diagnosis_code IS NULL
    ;
