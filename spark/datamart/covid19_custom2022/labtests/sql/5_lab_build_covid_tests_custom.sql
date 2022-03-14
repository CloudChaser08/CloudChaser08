SELECT
    record_id,
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_gender,
    patient_age	,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    date_service,
    date_specimen,
    date_report,
    time_report,
    loinc_code,
    test_id,
    test_number,
    test_battery_local_id,
    test_battery_std_id,
    test_battery_name,
    lab_id,
    test_ordered_local_id,
    test_ordered_std_id,
    test_ordered_name,
    result_id,
    result,
    result_name,
    result_unit_of_measure,
    result_desc,
    result_comments,
    ref_range,
    abnormal_flag,
    fasting_status,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_code_priority,
    procedure_code,
    procedure_code_qual,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    lab_npi,
    ordering_npi,
    payer_id,
    payer_id_qual,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    lab_other_id,
    lab_other_qual,
    lab_address_1,
    lab_address_2,
    lab_city,
    lab_state,
    lab_zip,
    ordering_other_id,
    ordering_other_qual,
    ordering_name,
    ordering_market_type,
    ordering_specialty,
    ordering_vendor_id,
    ordering_tax_id,
    ordering_dea_id,
    ordering_ssn,
    ordering_state_license,
    ordering_upin,
    ordering_commercial_id,
    ordering_address_1,
    ordering_address_2,
    ordering_city,
    ordering_state,
    ordering_zip,
    medication_generic_name,
    medication_dose,
    logical_delete_reason,
    vendor_record_id,
    claim_bucket_id,
    part_provider,
    part_mth
FROM
(
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id is null
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_1} AS INT) AND CAST({claim_bucket_id_up_1} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_2} AS INT) AND CAST({claim_bucket_id_up_2} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_3} AS INT) AND CAST({claim_bucket_id_up_3} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_4} AS INT) AND CAST({claim_bucket_id_up_4} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_5} AS INT) AND CAST({claim_bucket_id_up_5} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_6} AS INT) AND CAST({claim_bucket_id_up_6} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_7} AS INT) AND CAST({claim_bucket_id_up_7} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_8} AS INT) AND CAST({claim_bucket_id_up_8} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_9} AS INT) AND CAST({claim_bucket_id_up_9} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_10} AS INT) AND CAST({claim_bucket_id_up_10} AS INT)
UNION ALL
    SELECT ct1.* FROM lab_build_all_tests_custom ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_11} AS INT) AND CAST({claim_bucket_id_up_11} AS INT)
) ct
GROUP BY
    record_id,
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_gender,
    patient_age	,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    date_service,
    date_specimen,
    date_report,
    time_report,
    loinc_code,
    test_id,
    test_number,
    test_battery_local_id,
    test_battery_std_id,
    test_battery_name,
    lab_id,
    test_ordered_local_id,
    test_ordered_std_id,
    test_ordered_name,
    result_id,
    result,
    result_name,
    result_unit_of_measure,
    result_desc,
    result_comments,
    ref_range,
    abnormal_flag,
    fasting_status,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_code_priority,
    procedure_code,
    procedure_code_qual,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    lab_npi,
    ordering_npi,
    payer_id,
    payer_id_qual,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    lab_other_id,
    lab_other_qual,
    lab_address_1,
    lab_address_2,
    lab_city,
    lab_state,
    lab_zip,
    ordering_other_id,
    ordering_other_qual,
    ordering_name,
    ordering_market_type,
    ordering_specialty,
    ordering_vendor_id,
    ordering_tax_id,
    ordering_dea_id,
    ordering_ssn,
    ordering_state_license,
    ordering_upin,
    ordering_commercial_id,
    ordering_address_1,
    ordering_address_2,
    ordering_city,
    ordering_state,
    ordering_zip,
    medication_generic_name,
    medication_dose,
    logical_delete_reason,
    vendor_record_id,
    claim_bucket_id,
    part_provider,
    part_mth