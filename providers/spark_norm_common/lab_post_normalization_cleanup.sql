DROP TABLE IF EXISTS lab_cleanup;
CREATE TABLE lab_cleanup AS
SELECT monotonically_increasing_id() AS record_id,
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_gender,
    CASE WHEN CAST(patient_age as int) > 85 THEN '90' ELSE patient_age END AS patient_age,
    CASE
    WHEN date_service IS NULL or date_service = ''
    THEN NULL
    WHEN YEAR(date_service) - CAST(patient_year_of_birth AS int) > 80
    OR CAST(patient_age as int) > 85
    THEN CAST(YEAR(date_service) - 90 AS string)
    END,
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
    clean_up_diagnosis_code(diagnosis_code, diagnosis_code_qual, date_service) as diagnosis_code,
    diagnosis_code_qual,
    diagnosis_code_priorty,
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
    ordering_market_type,
    ordering_specialty
FROM lab_common_model
;

DROP TABLE lab_common_model;
ALTER TABLE lab_cleanup RENAME TO lab_common_model;
