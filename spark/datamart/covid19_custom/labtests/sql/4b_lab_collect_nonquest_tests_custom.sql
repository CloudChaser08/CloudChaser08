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
    lab_collect_tests
WHERE
    part_provider != 'quest'