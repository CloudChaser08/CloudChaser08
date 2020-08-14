SELECT
    ct.record_id
    ,ct.claim_id
    ,ct.hvid
    ,ct.created
    ,ct.model_version
    ,ct.data_set
    ,ct.data_feed
    ,ct.data_vendor
    ,ct.source_version
    ,ct.patient_gender
    ,ct.patient_age
    ,ct.patient_year_of_birth
    ,ct.patient_zip3
    ,ct.patient_state
    ,ct.date_service
    ,ct.date_specimen
    ,ct.date_report
    ,ct.time_report
    ,ct.loinc_code
    ,ct.lab_id
    ,ct.test_id
    ,ct.test_number
    ,ct.test_battery_local_id
    ,ct.test_battery_std_id
    ,ct.test_battery_name
    ,ct.test_ordered_local_id
    ,ct.test_ordered_std_id
    ,ct.test_ordered_name
    ,ct.result_id
    ,ct.result
    ,ct.result_name
    ,ct.result_unit_of_measure
    ,ct.result_desc
    ,ct.result_comments
    ,ct.ref_range
    ,ct.abnormal_flag
    ,ct.fasting_status
    ,ct.diagnosis_code
    ,ct.diagnosis_code_qual
    ,ct.diagnosis_code_priority
    ,ct.procedure_code
    ,ct.procedure_code_qual
    ,ct.procedure_modifier_1
    ,ct.procedure_modifier_2
    ,ct.procedure_modifier_3
    ,ct.procedure_modifier_4
    ,ct.lab_npi
    ,ct.ordering_npi
    ,ct.payer_id
    ,ct.payer_id_qual
    ,ct.payer_name
    ,ct.payer_parent_name
    ,ct.payer_org_name
    ,ct.payer_plan_id
    ,ct.payer_plan_name
    ,ct.payer_type
    ,ct.lab_other_id
    ,ct.lab_other_qual
    ,ct.lab_address_1
    ,ct.lab_address_2
    ,ct.lab_city
    ,ct.lab_state
    ,ct.lab_zip
    ,ct.ordering_other_id
    ,ct.ordering_other_qual
    ,ct.ordering_name
    ,ct.ordering_market_type
    ,ct.ordering_specialty
    ,ct.ordering_vendor_id
    ,ct.ordering_tax_id
    ,ct.ordering_dea_id
    ,ct.ordering_ssn
    ,ct.ordering_state_license
    ,ct.ordering_upin
    ,ct.ordering_commercial_id
    ,ct.ordering_address_1
    ,ct.ordering_address_2
    ,ct.ordering_city
    ,ct.ordering_state
    ,ct.ordering_zip
    ,ct.medication_generic_name
    ,ct.medication_dose
    ,ct.logical_delete_reason
    ,ct.vendor_record_id
    ,ct.claim_bucket_id
    ,ct.part_mth
    ,ct.part_provider
FROM
(
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id is null
UNION ALL
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_1} AS INT) AND CAST({claim_bucket_id_up_1} AS INT)
UNION ALL
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_2} AS INT) AND CAST({claim_bucket_id_up_2} AS INT)
UNION ALL
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_3} AS INT) AND CAST({claim_bucket_id_up_3} AS INT)
UNION ALL
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_4} AS INT) AND CAST({claim_bucket_id_up_4} AS INT)
UNION ALL
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_5} AS INT) AND CAST({claim_bucket_id_up_5} AS INT)
UNION ALL
    SELECT ct1.* FROM _temp_lab_all_tests ct1
    WHERE ct1.part_mth IN ({list_of_part_mth}) AND ct1.covid19_ind=1
        AND ct1.claim_bucket_id BETWEEN CAST({claim_bucket_id_low_6} AS INT) AND CAST({claim_bucket_id_up_6} AS INT)
) ct
GROUP BY
    ct.record_id
    ,ct.claim_id
    ,ct.hvid
    ,ct.created
    ,ct.model_version
    ,ct.data_set
    ,ct.data_feed
    ,ct.data_vendor
    ,ct.source_version
    ,ct.patient_gender
    ,ct.patient_age
    ,ct.patient_year_of_birth
    ,ct.patient_zip3
    ,ct.patient_state
    ,ct.date_service
    ,ct.date_specimen
    ,ct.date_report
    ,ct.time_report
    ,ct.loinc_code
    ,ct.lab_id
    ,ct.test_id
    ,ct.test_number
    ,ct.test_battery_local_id
    ,ct.test_battery_std_id
    ,ct.test_battery_name
    ,ct.test_ordered_local_id
    ,ct.test_ordered_std_id
    ,ct.test_ordered_name
    ,ct.result_id
    ,ct.result
    ,ct.result_name
    ,ct.result_unit_of_measure
    ,ct.result_desc
    ,ct.result_comments
    ,ct.ref_range
    ,ct.abnormal_flag
    ,ct.fasting_status
    ,ct.diagnosis_code
    ,ct.diagnosis_code_qual
    ,ct.diagnosis_code_priority
    ,ct.procedure_code
    ,ct.procedure_code_qual
    ,ct.procedure_modifier_1
    ,ct.procedure_modifier_2
    ,ct.procedure_modifier_3
    ,ct.procedure_modifier_4
    ,ct.lab_npi
    ,ct.ordering_npi
    ,ct.payer_id
    ,ct.payer_id_qual
    ,ct.payer_name
    ,ct.payer_parent_name
    ,ct.payer_org_name
    ,ct.payer_plan_id
    ,ct.payer_plan_name
    ,ct.payer_type
    ,ct.lab_other_id
    ,ct.lab_other_qual
    ,ct.lab_address_1
    ,ct.lab_address_2
    ,ct.lab_city
    ,ct.lab_state
    ,ct.lab_zip
    ,ct.ordering_other_id
    ,ct.ordering_other_qual
    ,ct.ordering_name
    ,ct.ordering_market_type
    ,ct.ordering_specialty
    ,ct.ordering_vendor_id
    ,ct.ordering_tax_id
    ,ct.ordering_dea_id
    ,ct.ordering_ssn
    ,ct.ordering_state_license
    ,ct.ordering_upin
    ,ct.ordering_commercial_id
    ,ct.ordering_address_1
    ,ct.ordering_address_2
    ,ct.ordering_city
    ,ct.ordering_state
    ,ct.ordering_zip
    ,ct.medication_generic_name
    ,ct.medication_dose
    ,ct.logical_delete_reason
    ,ct.vendor_record_id
    ,ct.claim_bucket_id
    ,ct.part_mth
    ,ct.part_provider
