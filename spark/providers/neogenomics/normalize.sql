-- insert all rows with diagnoses
INSERT INTO lab_common_model
SELECT
    NULL,                                    -- record_id
    t.test_order_id,                         -- claim_id
    mp.hvid,                                 -- hvid
    {today},                                 -- created
    '3',                                     -- model_version
    {filename},                              -- data_set
    {feedname},                              -- data_feed
    {vendor},                                -- data_vendor
    '1',                                     -- source_version
    mp.gender,                               -- patient_gender
    cap_age(mp.age),                         -- patient_age
    cap_year_of_birth(
        mp.age,
        CAST(extract_date(
                t.test_ordered_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
                ) AS DATE),
        mp.yearOfBirth
        ),                                   -- patient_year_of_birth
    mp.threeDigitZip,                        -- patient_zip3
    UPPER(mp.state),                         -- patient_state
    extract_date(
        t.test_ordered_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                   -- date_service
    extract_date(
        t.specimen_collected_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                   -- date_specimen
    COALESCE(
        extract_date(
            t.test_canceled_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
            ),
        extract_date(
            t.test_reported_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
            )
        ),                                   -- date_report
    NULL,                                    -- time_report
    NULL,                                    -- loinc_code
    NULL,                                    -- lab_id
    t.test_order_id,                         -- test_id
    NULL,                                    -- test_number
    t.panel_code,                            -- test_battery_local_id
    NULL,                                    -- test_battery_std_id
    t.panel_name,                            -- test_battery_name
    NULL,                                    -- test_ordered_local_id
    NULL,                                    -- test_ordered_std_id
    CONCAT(t.test_name, ' ', t.technology),  -- test_ordered_name
    NULL,                                    -- result_id
    NULL,                                    -- result
    NULL,                                    -- result_name
    NULL,                                    -- result_unit_of_measure
    NULL,                                    -- result_desc
    t.level_of_service,                      -- result_comments
    NULL,                                    -- ref_range_low
    NULL,                                    -- ref_range_high
    NULL,                                    -- ref_range_alpha
    NULL,                                    -- abnormal_flag
    NULL,                                    -- fasting_status
    clean_up_diagnosis_code(
        SPLIT(t.icd_code, ',')[n.n],
        NULL,
        CAST(extract_date(
                t.test_ordered_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
                ) as DATE)
        ),                                   -- diagnosis_code
    NULL,                                    -- diagnosis_code_qual
    n.n + 1,                                 -- diagnosis_code_priority
    NULL,                                    -- procedure_code
    NULL,                                    -- procedure_code_qual
    NULL,                                    -- lab_npi
    NULL,                                    -- ordering_npi
    NULL,                                    -- payer_id
    NULL,                                    -- payer_id_qual
    NULL,                                    -- payer_name
    NULL,                                    -- payer_parent_name
    NULL,                                    -- payer_org_name
    NULL,                                    -- payer_plan_id
    NULL,                                    -- payer_plan_name
    NULL,                                    -- payer_type
    NULL,                                    -- lab_other_id
    NULL,                                    -- lab_other_qual
    NULL,                                    -- ordering_other_id
    NULL,                                    -- ordering_other_qual
    NULL,                                    -- ordering_name
    NULL,                                    -- ordering_market_type
    NULL,                                    -- ordering_specialty
    NULL,                                    -- ordering_vendor_id
    NULL,                                    -- ordering_tax_id
    NULL,                                    -- ordering_dea_id
    NULL,                                    -- ordering_ssn
    NULL,                                    -- ordering_state_license
    NULL,                                    -- ordering_upin
    NULL,                                    -- ordering_commercial_id
    NULL,                                    -- ordering_address_1
    NULL,                                    -- ordering_address_2
    NULL,                                    -- ordering_city
    NULL,                                    -- ordering_state
    t.client_zip,                            -- ordering_zip
    CASE WHEN UPPER(t.status) = 'CANCELLED'
    THEN 'CANCELED'
    ELSE NULL
    END                                      -- logical_delete_reason
FROM transactional_tests t
    LEFT JOIN matching_payload mp ON UPPER(t.patient_id) = UPPER(mp.personid)
    CROSS JOIN diagnosis_exploder n

-- implicit here is that t.icd_code itself is not null or blank
WHERE SPLIT(TRIM(t.icd_code),',')[n.n] IS NOT NULL
    AND SPLIT(TRIM(t.icd_code),',')[n.n] != ''
    ;

-- insert all rows with diagnoses
INSERT INTO lab_common_model
SELECT
    NULL,                                    -- record_id
    t.test_order_id,                         -- claim_id
    mp.hvid,                                 -- hvid
    {today},                                 -- created
    '3',                                     -- model_version
    {filename},                              -- data_set
    {feedname},                              -- data_feed
    {vendor},                                -- data_vendor
    '1',                                     -- source_version
    mp.gender,                               -- patient_gender
    cap_age(mp.age),                         -- patient_age
    cap_year_of_birth(
        mp.age,
        CAST(extract_date(
                t.test_ordered_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
                ) AS DATE),
        mp.yearOfBirth
        ),                                   -- patient_year_of_birth
    mp.threeDigitZip,                        -- patient_zip3
    UPPER(mp.state),                         -- patient_state
    extract_date(
        t.test_ordered_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                   -- date_service
    extract_date(
        t.specimen_collected_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                   -- date_specimen
    COALESCE(
        extract_date(
            t.test_canceled_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
            ),
        extract_date(
            t.test_reported_date, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
            )
        ),                                   -- date_report
    NULL,                                    -- time_report
    NULL,                                    -- loinc_code
    NULL,                                    -- lab_id
    t.test_order_id,                         -- test_id
    NULL,                                    -- test_number
    t.panel_code,                            -- test_battery_local_id
    NULL,                                    -- test_battery_std_id
    t.panel_name,                            -- test_battery_name
    NULL,                                    -- test_ordered_local_id
    NULL,                                    -- test_ordered_std_id
    CONCAT(t.test_name, ' ', t.technology),  -- test_ordered_name
    NULL,                                    -- result_id
    NULL,                                    -- result
    NULL,                                    -- result_name
    NULL,                                    -- result_unit_of_measure
    NULL,                                    -- result_desc
    t.level_of_service,                      -- result_comments
    NULL,                                    -- ref_range_low
    NULL,                                    -- ref_range_high
    NULL,                                    -- ref_range_alpha
    NULL,                                    -- abnormal_flag
    NULL,                                    -- fasting_status
    NULL,                                    -- diagnosis_code
    NULL,                                    -- diagnosis_code_qual
    NULL,                                    -- diagnosis_code_priority
    NULL,                                    -- procedure_code
    NULL,                                    -- procedure_code_qual
    NULL,                                    -- lab_npi
    NULL,                                    -- ordering_npi
    NULL,                                    -- payer_id
    NULL,                                    -- payer_id_qual
    NULL,                                    -- payer_name
    NULL,                                    -- payer_parent_name
    NULL,                                    -- payer_org_name
    NULL,                                    -- payer_plan_id
    NULL,                                    -- payer_plan_name
    NULL,                                    -- payer_type
    NULL,                                    -- lab_other_id
    NULL,                                    -- lab_other_qual
    NULL,                                    -- ordering_other_id
    NULL,                                    -- ordering_other_qual
    NULL,                                    -- ordering_name
    NULL,                                    -- ordering_market_type
    NULL,                                    -- ordering_specialty
    NULL,                                    -- ordering_vendor_id
    NULL,                                    -- ordering_tax_id
    NULL,                                    -- ordering_dea_id
    NULL,                                    -- ordering_ssn
    NULL,                                    -- ordering_state_license
    NULL,                                    -- ordering_upin
    NULL,                                    -- ordering_commercial_id
    NULL,                                    -- ordering_address_1
    NULL,                                    -- ordering_address_2
    NULL,                                    -- ordering_city
    NULL,                                    -- ordering_state
    t.client_zip,                            -- ordering_zip
    CASE WHEN UPPER(t.status) = 'CANCELLED'
    THEN 'CANCELED'
    ELSE NULL
    END                                      -- logical_delete_reason
FROM transactional_tests t
    LEFT JOIN matching_payload mp ON UPPER(t.patient_id) = UPPER(mp.personid)

WHERE t.icd_code IS NULL
    OR t.icd_code = ''
    ;
