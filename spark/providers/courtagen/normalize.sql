CREATE TEMPORARY VIEW transactions_w_diag_concat AS
SELECT *,
CONCAT(
    -- ICD9 and ICD10 code fields contain the code followed by a space
    -- and a description of the code. Remove the description before
    -- concatenating them together
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd9_code_1), ' .*', '')), '_01'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd9_code_2), ' .*', '')), '_01'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd9_code_3), ' .*', '')), '_01'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd9_code_4), ' .*', '')), '_01'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd10_code_1), ' .*', '')), '_02'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd10_code_2), ' .*', '')), '_02'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd10_code_3), ' .*', '')), '_02'), ':',
    CONCAT(TRIM(REGEXP_REPLACE(TRIM(icd10_code_4), ' .*', '')), '_02')
) as diag_concat,
CASE WHEN
    (icd9_code_1 IS NULL OR TRIM(icd9_code_1) == '') AND
    (icd9_code_2 IS NULL OR TRIM(icd9_code_2) == '') AND
    (icd9_code_3 IS NULL OR TRIM(icd9_code_3) == '') AND
    (icd9_code_4 IS NULL OR TRIM(icd9_code_4) == '') AND
    (icd10_code_1 IS NULL OR TRIM(icd10_code_1) == '') AND
    (icd10_code_2 IS NULL OR TRIM(icd10_code_2) == '') AND
    (icd10_code_3 IS NULL OR TRIM(icd10_code_3) == '') AND
    (icd10_code_4 IS NULL OR TRIM(icd10_code_4) == '')
    THEN true
    ELSE false
END AS no_diag
FROM transactions_raw;

-- insert all rows with diagnoses
INSERT INTO lab_common_model
SELECT
    NULL,                                   -- record_id
    t.claimid,                              -- claim_id
    mp.hvid,                                -- hvid
    {today},                                -- created
    '2',                                    -- model_version
    {filename},                             -- data_set
    {feedname},                             -- data_feed
    {vendor},                               -- data_vendor
    NULL,                                   -- source_version
    CASE WHEN UPPER(t.gender) = 'MALE' THEN 'M'
        WHEN UPPER(t.gender) = 'FEMALE' THEN 'F'
        ELSE 'U' END,                       -- patient_gender
    regexp_replace(age_at_test, '\\..*', ''),
                                            -- patient_age
    mp.yearOfBirth,                         -- patient_year_of_birth
    mp.threeDigitZip,                       -- patient_zip3
    state_abbr.abbr,                        -- patient_state
    extract_date(
        rundate, '%Y-%m-%d %H:%M:%S %Z', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- date_service
    NULL,                                   -- date_specimen
    NULL,                                   -- date_report
    NULL,                                   -- time_report
    NULL,                                   -- loinc_code
    NULL,                                   -- lab_id
    CONCAT(
        TRIM(panel), ' ', TRIM(panel_version)
    ),                                      -- test_id
    NULL,                                   -- test_number
    NULL,                                   -- test_battery_local_id
    NULL,                                   -- test_battery_std_id
    NULL,                                   -- test_battery_name
    NULL,                                   -- test_ordered_local_id
    NULL,                                   -- test_ordered_std_id
    NULL,                                   -- test_ordered_name
    NULL,                                   -- result_id
    substring(summ_call, 0, 1),             -- result
    summ.name,                              -- result_name
    NULL,                                   -- result_unit_of_measure
    summ.desc,                              -- result_desc
    NULL,                                   -- result_comments
    NULL,                                   -- ref_range_low
    NULL,                                   -- ref_range_high
    NULL,                                   -- ref_range_alpha
    NULL,                                   -- abnormal_flag
    NULL,                                   -- fasting_status
    SPLIT(SPLIT(diag_concat, ':')[n], '_')[0],
                                            -- diagnosis_code
    SPLIT(SPLIT(diag_concat, ':')[n], '_')[1],
                                            -- diagnosis_code_qual
    NULL,                                   -- diagnosis_code_priority
    NULL,                                   -- procedure_code
    NULL,                                   -- procedure_code_qual
    NULL,                                   -- lab_npi
    ordering_physician_npi,                 -- ordering_npi
    NULL,                                   -- payer_id
    NULL,                                   -- payer_id_qual
    CASE WHEN primary_insurance_company IS NOT NULL
        AND TRIM(primary_insurance_company) != ''
        THEN primary_insurance_company
        WHEN secondary_insurance_company IS NOT NULL
        AND TRIM(secondary_insurance_company) != ''
        THEN secondary_insurance_company
        ELSE NULL END,                      -- payer_name
    NULL,                                   -- payer_parent_name
    NULL,                                   -- payer_org_name
    NULL,                                   -- payer_plan_id
    NULL,                                   -- payer_plan_name
    billing_type,                           -- payer_type
    NULL,                                   -- lab_other_id
    NULL,                                   -- lab_other_qual
    doc_id,                                 -- ordering_other_id
    'VENDOR_PROPRIETARY',                   -- ordering_other_qual
    ordering_physician,                     -- ordering_name
    NULL,                                   -- ordering_market_type
    NULL                                    -- ordering_specialty
FROM transactions_w_diag_concat t
    LEFT JOIN matching_payload mp USING (hvJoinKey)
    LEFT JOIN state_abbr ON UPPER(t.state) = state_abbr.state
    LEFT JOIN summ ON substring(summ_call, 0, 1) = summ.value
    CROSS JOIN diagnosis_exploder
WHERE
    patient_country = 'United States'
    AND SPLIT(SPLIT(diag_concat, ':')[n], '_')[0] != '';

-- insert all rows with diagnoses
INSERT INTO lab_common_model
SELECT
    NULL,                                   -- record_id
    t.claimid,                              -- claim_id
    mp.hvid,                                -- hvid
    {today},                                -- created
    '2',                                    -- model_version
    {filename},                             -- data_set
    {feedname},                             -- data_feed
    {vendor},                               -- data_vendor
    NULL,                                   -- source_version
    CASE WHEN UPPER(t.gender) = 'MALE' THEN 'M'
        WHEN UPPER(t.gender) = 'FEMALE' THEN 'F'
        ELSE 'U' END,                       -- patient_gender
    regexp_replace(age_at_test, '\\..*', ''),
                                            -- patient_age
    mp.yearOfBirth,                         -- patient_year_of_birth
    mp.threeDigitZip,                       -- patient_zip3
    state_abbr.abbr,                        -- patient_state
    extract_date(
        rundate, '%Y-%m-%d %H:%M:%S %Z', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- date_service
    NULL,                                   -- date_specimen
    NULL,                                   -- date_report
    NULL,                                   -- time_report
    NULL,                                   -- loinc_code
    NULL,                                   -- lab_id
    CONCAT(
        TRIM(panel), ' ', TRIM(panel_version)
    ),                                      -- test_id
    NULL,                                   -- test_number
    NULL,                                   -- test_battery_local_id
    NULL,                                   -- test_battery_std_id
    NULL,                                   -- test_battery_name
    NULL,                                   -- test_ordered_local_id
    NULL,                                   -- test_ordered_std_id
    NULL,                                   -- test_ordered_name
    NULL,                                   -- result_id
    substring(summ_call, 0, 1),             -- result
    summ.name,                              -- result_name
    NULL,                                   -- result_unit_of_measure
    summ.desc,                              -- result_desc
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
    ordering_physician_npi,                 -- ordering_npi
    NULL,                                   -- payer_id
    NULL,                                   -- payer_id_qual
    CASE WHEN primary_insurance_company IS NOT NULL
        AND TRIM(primary_insurance_company) != ''
        THEN primary_insurance_company
        WHEN secondary_insurance_company IS NOT NULL
        AND TRIM(secondary_insurance_company) != ''
        THEN secondary_insurance_company
        ELSE NULL END,                      -- payer_name
    NULL,                                   -- payer_parent_name
    NULL,                                   -- payer_org_name
    NULL,                                   -- payer_plan_id
    NULL,                                   -- payer_plan_name
    billing_type,                           -- payer_type
    NULL,                                   -- lab_other_id
    NULL,                                   -- lab_other_qual
    doc_id,                                 -- ordering_other_id
    'VENDOR_PROPRIETARY',                   -- ordering_other_qual
    ordering_physician,                     -- ordering_name
    NULL,                                   -- ordering_market_type
    NULL                                    -- ordering_specialty
FROM transactions_w_diag_concat t
    LEFT JOIN matching_payload mp USING (hvJoinKey)
    LEFT JOIN state_abbr ON UPPER(t.state) = state_abbr.state
    LEFT JOIN summ ON substring(summ_call, 0, 1) = summ.value
WHERE
    patient_country = 'United States'
    AND no_diag;
