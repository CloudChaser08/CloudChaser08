from pyspark.sql.types import StructType, StructField, StringType

demographic = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
        'id', 'emr_patient_id', 'practice_id', 'system_id', 'patient_id', 'first_name', 'last_name', 'birth_date',
        'gender', 'race', 'patient_address_one', 'patient_address_two', 'patient_address_three', 'city', 'state',
        'zip_code', 'zip_code_plus_four', 'date_of_death', 'middle_initial', 'middle_name', 'ethnicity', 'home_phone',
        'home_phone_note', 'marital_status', 'ssn', 'title', 'patient_language', 'mobile_phone', 'mobile_phone_note',
        'work_phone', 'work_phone_note', 'smoker_status', 'alcohol_use', 'patient_weight', 'patient_height', 'country',
        'doctor_name', 'bsa', 'first_appointment_date', 'suffix', 'icd_nine', 'icd_ten', 'patient_alive_indicator',
        'test_patient_indicator', 'maiden_name', 'clinical_trial_indicator', 'patient_age', 'allergy_indicator',
        'infection_indicator', 'inpatient_indicator', 'county', 'faxphone', 'cur_entry', 'email_address', 'occupation',
        'educational_level', 'present_employer_name', 'moms_full_name', 'fathers_full_name', 'moms_maiden_name',
        'birth_place', 'location_code', 'date_inactivated', 'date_reactivated', 'order_payor_type', 'order_payor_name',
        'drivers_license_number', 'drivers_license_state', 'allergen_description', 'plan_one_name',
        'primary_coverage_type', 'plan_one_pay_method_type_text', 'plan_one_priority', 'plan_one_start_date',
        'plan_one_end_date', 'plan_one_card_holder_id', 'plan_one_group_number', 'plan_one_expiration_date',
        'plan_one_group_code', 'plan_two_name', 'secondary_coverage_type', 'plan_two_pay_method_type_text',
        'plan_two_priority', 'plan_two_start_date', 'plan_two_end_date', 'plan_two_card_holder_id',
        'plan_two_group_number', 'plan_two_expiration_date', 'plan_two_group_code', 'plan_three_name',
        'tertiary_coverage_type', 'plan_three_pay_method_type_text', 'plan_three_priority', 'plan_three_start_date',
        'plan_three_end_date', 'plan_three_card_holder_id', 'plan_three_group_number', 'plan_three_expiration_date',
        'plan_three_group_code', 'hvJoinKey'
    ]
])

diagnosis = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
        'id', 'emr_patient_id', 'practice_id', 'system_id', 'patient_id', 'diagnosis_id', 'icd_cd', 'stage_of_disease',
        'diagnosis_date', 'diagnosis_typ', 'confirm_diagnosis', 'resolution_date', 'diagnosis_name', 'valid_entry',
        'diagnosis_desc', 'mthd_of_diagnosis', 'resolution_desc', 'stg_crit_desc', 'cur_entry_ind', 'clinical_desc',
        'diagnosis_cmt', 'vision_diagnosis_id', 'provider_npi', 'icd10', 'diag_group', 'cancer_stage', 'cancer_stage_t',
        'cancer_stage_n', 'cancer_stage_m'
    ]
])

encounter = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
        'id', 'emr_patient_id', 'practice_id', 'system_id', 'patient_id', 'patient_visit_id', 'visit_type',
        'visit_date', 'visit_complete', 'inst_id', 'userid_link', 'treatment_plan_name', 'treatment_plan_vers_no',
        'date_treatment_plan_init', 'cycle_no', 'cycle_day', 'visit_inst_id', 'sch_cmt', 'visit_start_tstamp',
        'visit_end_tstamp', 'cancel_reason_type', 'patient_instr_cmt', 'visit_financial_status_cmt', 'treatment_name',
        'treatment_description', 'cpt', 'visit_billed_amount', 'visit_unit_of_measure', 'claim_id', 'contract_amt',
        'reimburse_amt', 'provider_npi', 'rapid_3', 'cdai', 'sdai', 'das28', 'haq_score', 'total_normal_28joint',
        'total_tender_28joint', 'total_swollen_28joint', 'pain_scale', 'fn_raw', 'fn_dec', 'mdglobal_scale', 'ptgl_dec'
    ]
])

lab = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
        'id', 'emr_patient_id', 'practice_id', 'system_id', 'patient_id', 'test_id', 'test_name', 'test_date',
        'test_result_group_id', 'test_result_id', 'test_type', 'test_name_specific', 'test_value',
        'test_value_string', 'test_value_txt', 'min_norm', 'max_norm', 'unit_of_measure', 'unit_desc', 'min_reason',
        'max_reason', 'valid_entry_ind', 'abnormal_flag_cd', 'notes', 'physician_id', 'cpt', 'delivery_cpt',
        'record_source_rto_execution_date', 'end_date', 'record_source'
    ]
])

order_dispense = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
        'id', 'emr_patient_id', 'practice_id', 'system_id', 'patient_id', 'visit_id', 'prescription_number', 'item_no',
        'agt_seq_no', 'erx_agt_rx_id', 'ordersall_medications_flag', 'edw_evaluated_ind', 'npi', 'brand_name',
        'generic_name', 'duration', 'admin_date', 'num_doses', 'icd_nine', 'icd_ten', 'qty', 'days_supply', 'refills',
        'strength', 'agt_rx_strength', 'ndc_written', 'ndc_labeler_code', 'ndc_drug_and_strength', 'ndc_package_size',
        'quantity_per_day', 'quantity_units', 'instructions', 'discontinue_date', 'type', 'create_date',
        'created_by_name', 'ordered_by_name', 'planned_duration', 'planned_unit', 'actual_unit', 'planned_value',
        'actual_value', 'route', 'waste', 'cpt_code', 'rx_cmt', 'valid_entry_ind', 'treatment_plan_name',
        'treatment_plan_vers_no', 'phase_seq_no', 'cycle_no', 'cycle_day', 'completed_ind', 'dispensed_ind',
        'treatment_start_date', 'treatment_line', 'treatment_intent', 'treatment_use', 'rx_calc_audit_desc',
        'err_rsn_txt', 'dosage_form', 'dose_level', 'rx_total', 'admn_dosage_unit', 'admn_route', 'admn_dose_frq_unit',
        'admn_frq_x', 'admn_frq_unit', 'admn_dur_unit', 'general_orders', 'discontinue_reason',
        'discontinue_effective_date', 'rx_dose_range', 'strength_unit', 'set_date_treatment_plan_init', 'course_desc',
        'order_unit', 'order_dose', 'infusion_typ', 'infusion_duration', 'infusion_timescale', 'dose_strength',
        'agt_vol', 'agt_vol_uom', 'dose_pct', 'dispense_vol', 'dispense_unit', 'admn_dose_no_rec', 'admn_dose_total',
        'reason_desc', 'inst_id', 'disp_qty', 'dest_pharm_id', 'sent_ind', 'received_ind', 'received_date',
        'drug_description', 'dose_form_description', 'prescribed_else_ind', 'uom_description', 'status',
        'encounter_date', 'scheduled_date', 'subclass', 'completed_date', 'cancelled_ind', 'cancelled_date'
    ]
])

enriched = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
        'drug_group', 'practice_name', 'status', 'provider_name'
    ]
])
