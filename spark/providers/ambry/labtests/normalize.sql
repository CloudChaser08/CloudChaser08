INSERT INTO labtests_common_model
SELECT
    NULL,                   -- record_id
    t.accession_number,     -- claim_id
    p.hvid,                 -- hvid
    NULL,                   -- created
    NULL,                   -- model_version
    NULL,                   -- data_set
    NULL,                   -- data_feed
    NULL,                   -- data_vendor
    NULL,                   -- source_version
    UPPER(TRIM(COALESCE(
                p.gender,
                t.gender,
                'U'
    ))),                    -- patient_gender
    NULL,                   -- patient_age
    COALESCE(p.yearOfBirth,
             YEAR(t.date_of_birth)
    ),                      -- patient_year_of_birth
    SUBSTR(
        TRIM(COALESCE(p.threeDigitZip, t.patient_zip_code)),
        1,
        3
    )                       -- patient_zip3
    TRIM(UPPER(COALESCE(
        p.state,
        ''
    ))),                    -- patient_state
    extract_date(
        t.order_date,
        '%Y/%m/%d'
        {min_date},
        {max_date}
    ),                      -- date_service
    NULL,                   -- date_specimen
    extract_date(
        t.reported_date,
        '%Y/%m/%d'
        {min_date},
        {max_date}
    ),                      -- date_report
    NULL,                   -- time_report
    NULL,                   -- loinc_code
    NULL,                   -- lab_id
    NULL,                   -- test_id
    NULL,                   -- test_number
    NULL,                   -- test_battery_local_id
    NULL,                   -- test_battery_std_id
    NULL,                   -- test_battery_name
    NULL,                   -- test_ordered_local_id
    NULL,                   -- test_ordered_std_id
    NULL,                   -- test_ordered_name
    NULL,                   -- result_id
    NULL,                   -- result
    NULL,                   -- result_name
    NULL,                   -- result_unit_of_measure
    NULL,                   -- result_desc
    NULL,                   -- result_comments
    NULL,                   -- ref_range_low
    NULL,                   -- ref_range_high
    NULL,                   -- ref_range_alpha
    NULL,                   -- abnormal_flag
    NULL,                   -- fasting_status
    TRIM(UPPER(
    ARRAY(t.icd10_1, t.icd10_2, t.icd10_3, t.icd10_4, t.icd10_5, t.icd10_6,
        t.icd10_7, t.icd10_8, t.icd10_9, t.icd10_10, t.icd10_11, t.icd10_12)[e.n]
    )),                     -- diagnosis_code
    NULL,                   -- diagnosis_code_qual
    e.n + 1,                   -- diagnosis_code_priority
    NULL,                   -- procedure_code
    NULL,                   -- procedure_code_qual
    NULL,                   -- lab_npi
    t.npi,                  -- ordering_npi
    NULL,                   -- payer_id
    NULL,                   -- payer_id_qual
    NULL,                   -- payer_name
    NULL,                   -- payer_parent_name
    NULL,                   -- payer_org_name
    NULL,                   -- payer_plan_id
    NULL,                   -- payer_plan_name
    NULL,                   -- payer_type
    NULL,                   -- lab_other_id
    NULL,                   -- lab_other_qual
    NULL,                   -- ordering_other_id
    NULL,                   -- ordering_other_qual
    t.ordering_provider,    -- ordering_name
    NULL,                   -- ordering_market_type
    NULL,                   -- ordering_specialty
    NULL,                   -- ordering_vendor_id
    NULL,                   -- ordering_tax_id
    NULL,                   -- ordering_dea_id
    NULL,                   -- ordering_ssn
    NULL,                   -- ordering_state_license
    NULL,                   -- ordering_upin
    NULL,                   -- ordering_commercial_id
    NULL,                   -- ordering_address_1
    NULL,                   -- ordering_address_2
    NULL,                   -- ordering_city
    NULL,                   -- ordering_state
    NULL,                   -- ordering_zip
    NULL                    -- logical_delete_reason
FROM ambry_transactions t
    LEFT OUTER JOIN payload p
    CROSS JOIN exploder e
    ON t.hvJoinKey = p.hvJoinKey
WHERE
    -- Filter out cases from explosion where diagnosis_code is NULL
    (
        ARRAY(t.icd10_1, t.icd10_2, t.icd10_3, t.icd10_4, t.icd10_5, t.icd10_6,
            t.icd10_7, t.icd10_8, t.icd10_9, t.icd10_10, t.icd10_11, t.icd10_12)[e.n]
        IS NOT NULL
    )
    OR
    -- If all are NULL, include one row w/ NULL diagnosis_code
    (
        COALESCE(t.icd10_1, t.icd10_2, t.icd10_3, t.icd10_4, t.icd10_5, t.icd10_6,
            t.icd10_7, t.icd10_8, t.icd10_9, t.icd10_10, t.icd10_11, t.icd10_12) IS NULL
        AND
        e.n = 0
    )
;
