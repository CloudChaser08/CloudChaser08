DROP VIEW IF EXISTS default.labtests;
CREATE VIEW default.labtests (
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
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        date_service,
        date_specimen,
        date_report,
        time_report,
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
        result_id,
        result,
        result_name,
        result_unit_of_measure,
        result_desc,
        result_comments,
        ref_range_low,
        ref_range_high,
        ref_range_alpha,
        abnormal_flag,
        fasting_status,
        diagnosis_code,
        diagnosis_code_qual,
        diagnosis_code_priority,
        procedure_code,
        procedure_code_qual,
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
        logical_delete_reason,
        part_provider,
        part_best_date
        ) AS SELECT 
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
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    date_service,
    date_specimen,
    date_report,
    time_report,
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
    result_id,
    result,
    result_name,
    result_unit_of_measure,
    result_desc,
    result_comments,
    ref_range_low,
    ref_range_high,
    ref_range_alpha,
    abnormal_flag,
    fasting_status,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_code_priority,
    procedure_code,
    procedure_code_qual,
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
    logical_delete_reason,
    part_provider,
    CASE
    WHEN part_best_date IN ('NULL', '0_PREDATES_HVM_HISTORY')
    THEN '0_PREDATES_HVM_HISTORY'
    ELSE part_best_date
    END as part_best_date
FROM labtests_20170216
    ;