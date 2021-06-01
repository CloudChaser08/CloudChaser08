from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'header': SourceTable(
        'csv',
        separator='|',
        trimmify_nullify=True,
        columns=[
            'record_type',
            'client_number',
            'client_name',
            'create_date',
            'create_time',
            'version_code',
            'entity_id',
            'provider_key',
            'billing_or_pay_to_provider_taxonomy_code',
            'billing_prov_organization_name_or_billing_prov_last_name',
            'billing_prov_last_name',
            'billing_prov_first_name',
            'billing_prov_mi',
            'billing_prov_id_qual',
            'billing_prov_tax_id',
            'billing_prov_npi',
            'billing_providers_address_1',
            'billing_providers_address_2',
            'billing_providers_city',
            'billing_providers_state',
            'billing_providers_zip',
            'pay_to_provider_last_name',
            'pay_to_provider_first_name',
            'pay_to_provider_middle_initial',
            'pay_to_prov_id_qual',
            'pay_to_prov_tax_id',
            'pay_to_prov_npi',
            'pay_to_address_1',
            'pay_to_address_2',
            'pay_to_city',
            'pay_to_state',
            'pay_to_zip',
            'patient_relationship_to_insured',
            'group_number',
            'group_name',
            'insurance_type_code',
            'source_of_payment',
            'patient_date_of_death',
            'patient_weight',
            'patient_pregnancy_indicator',
            'patient_primary_id',
            'insured_primary_id',
            'insured_key',
            'patient_state',
            'patient_zip_code',
            'insured_state',
            'insured_zip_code',
            'patient_dob',
            'insured_dob',
            'patient_sex_',
            'insured_sex',
            'marital_status_code',
            'patient_key',
            'primary_payer_name',
            'primary_payer_tspid',
            'primary_payer_address_1',
            'primary_payer_address_2',
            'primary_payer_city',
            'primary_payer_state',
            'primary_payer_zip',
            'patient_control_number',
            'total_claim_charge_amount',
            'claim_frequency_code',
            'provider_signature_indicator',
            'medicare_assignment_code',
            'assignment_benefits_indicator',
            'release_of_information_indicator',
            'patient_signature_source',
            'work_related_indicator',
            'auto_accident_indicator',
            'other_accident_indicator',
            'auto_accident_state',
            'special_program_indicator',
            'participation_agreement',
            'delay_reason_code',
            'initial_treatment_date',
            'date_last_seen',
            'onset_of_current_illness_or_injury_date',
            'same_or_similar_symptom_date',
            'accident_date',
            'accident_hour',
            'last_menstrual_period_date',
            'prescription_date',
            'disability_from_date',
            'disability_to_date',
            'date_last_worked',
            'return_to_work_date',
            'admission_date',
            'discharge_date',
            'provider_care_date',
            'provider_relinquish_date',
            'contract_type_code',
            'contract_amount',
            'contract_percent',
            'contract_code',
            'contract_discount_percent',
            'contract_version_id',
            'patient_amount_paid',
            'total_lab_charges',
            'auth_exception_code',
            'mammography_certification_number',
            'referral_number',
            'prior_auth_number',
            'claim_original_reference_number',
            'clinical_laboratory_improvement_amendments_clia_identification_number',
            'investigational_device_exemption_number',
            'payerpath_claim_number',
            'medical_record_number',
            'demonstration_project_identifier',
            'type_of_transport',
            'transported_to_for',
            'miles',
            'purpose_of_round_trip',
            'purpose_of_stretcher',
            'amb_cond_code1',
            'amb_cond_code2',
            'amb_cond_code3',
            'amb_cond_code4',
            'amb_cond_code5',
            'code_category',
            'certification_condition_indicator',
            'vision_condition_code_1',
            'vision_condition_code_2',
            'vision_condition_code_3',
            'vision_condition_code_4',
            'vision_condition_code_5',
            'epsdt_referral_indicator',
            'epsdt_condition_code',
            'diagnosis_code_1',
            'diagnosis_code_2',
            'diagnosis_code_3',
            'diagnosis_code_4',
            'diagnosis_code_5',
            'diagnosis_code_6',
            'diagnosis_code_7',
            'diagnosis_code_8',
            'pricing_methodology',
            'repriced_allowed_amount',
            'repriced_saving_amount',
            'repricing_organization_identifier',
            'repricing_per_diem_or_flat_rate_amount',
            'repriced_approved_ambulatory_patient_group_code',
            'repriced_approved_ambulatory_patient_group_amount',
            'reject_reason_code',
            'policy_compliance_code',
            'exception_code',
            'referring_provider_last_name',
            'referring_provider_first_name',
            'referring_provider_middle_initial',
            'referring_provider_primary_id_qualifier',
            'referring_provider_primary_id',
            'referring_prov_npi_',
            'referring_provider_taxonomy_code',
            'rendering_provider_last',
            'rendering_provider_first',
            'rendering_provider_middle',
            'rendering_provider_primary_id_qualifier',
            'rendering_provider_primary_id',
            'rendering_provider_npi',
            'rendering_provider_specialty_code',
            'purchased_service_last_name',
            'purchased_service_first_name',
            'purchased_service_mi',
            'purchase_service_provider_id_qualifier',
            'purchase_service_provider_primary_identifier',
            'purchase_service_prov_npi',
            'facility_laboratory_type',
            'facility_laboratory_name',
            'facility_lab_primary_id_qualifier',
            'facility_laboratory_primary_identifier',
            'facility_lab_npi',
            'facility_laboratory_street_address_1',
            'facility_laboratory_street_address_2',
            'facility_laboratory_city',
            'facility_laboratory_state',
            'facility_laboratory_zip_code',
            'supervising_provider_last_name',
            'supervising_provider_first_name',
            'supervising_provider_middle_initial',
            'supervising_provider_primary_id_qualifier',
            'supervising_provider_primary_identifier',
            'supervising_prov_npi',
            'secondary_payer_sequence_number',
            'secondary_payer_patient_relationship_to_insured',
            'secondary_payer_group_number',
            'secondary_payer_group_name',
            'secondary_payer_insurance_type_code',
            'secondary_payer_source_of_payment',
            'teritary_payer_sequence_number',
            'teritary_payer_patient_relationship_to_insured',
            'teritary_payer_group_number',
            'teritary_payer_group_name',
            'teritary_payer_insurance_type_code',
            'teritary_payer_source_of_payment',
            '2nd_payer_grp_1_claim_group_code_1',
            '2nd_payer_grp_1_claim_reason_code_1',
            '2nd_payer_grp_1_dollar_amount_1',
            '2nd_payer_grp_1_adjustment_quantity_1',
            '2nd_payer_grp_1claim_reason_code_2',
            '2nd_payer_grp_1_dollar_amount_2',
            '2nd_payer_grp_1_adjustment_quantity_2',
            '2nd_payer_grp_1_claim_reason_code_3',
            '2nd_payer_grp_1_dollar_amount_3',
            '2nd_payer_grp_1adjustment_quantity_3',
            '2nd_payer_grp_1_claim_reason_code_4',
            '2nd_payer_grp_1_dollar_amount_4',
            '2nd_payer_grp_1_adjustment_quantity_4',
            '2nd_payer_grp_1_claim_reason_code_5',
            '2nd_payer_grp_1_dollar_amount_5',
            '2nd_payer_grp_1_adjustment_quantity_5',
            '2nd_payer_grp_1_claim_reason_code_6',
            '2nd_payer_grp_1_dollar_amount_6',
            '2nd_payer_grp_1_adjustment_quantity_6',
            '2nd_payer_grp_2_claim_group_code_2',
            '2nd_payer_grp_2_claim_reason_code_1',
            '2nd_payer_grp_2_dollar_amount_1',
            '2nd_payer_grp_2_adjustment_quantity_1',
            '2nd_payer_grp_2_claim_reason_code_2',
            '2nd_payer_grp_2_dollar_amount_2',
            '2nd_payer_grp_2_adjustment_quantity_2',
            '2nd_payer_grp_2_claim_reason_code_3',
            '2nd_payer_grp_2_dollar_amount_3',
            '2nd_payer_grp_2_adjustment_quantity_3',
            '2nd_payer_grp_2_claim_reason_code_4',
            '2nd_payer_grp_2_dollar_amount_4',
            '2nd_payer_grp_2_adjustment_quantity_4',
            '2nd_payer_grp_2_claim_reason_code_5',
            '2nd_payer_grp_2_dollar_amount_5',
            '2nd_payer_grp_2_adjustment_quantity_5',
            '2nd_payer_grp_2_claim_reason_code_6',
            '2nd_payer_grp_2_dollar_amount_6',
            '2nd_payer_grp_2_adjustment_quantity_6',
            '2nd_payer_grp_3_claim_group_code_3',
            '2nd_payer_grp_3_claim_reason_code_1',
            '2nd_payer_grp_3_dollar_amount_1',
            '2nd_payer_grp_3_adjustment_quantity_1',
            '2nd_payer_grp_3_claim_reason_code_2',
            '2nd_payer_grp_3_dollar_amount_2',
            '2nd_payer_grp_3_adjustment_quantity_2',
            '2nd_payer_grp_3_claim_reason_code_3',
            '2nd_payer_grp_3_dollar_amount_3',
            '2nd_payer_grp_3_adjustment_quantity_3',
            '2nd_payer_grp_3_claim_reason_code_4',
            '2nd_payer_grp_3_dollar_amount_4',
            '2nd_payer_grp_3_adjustment_quantity_4',
            '2nd_payer_grp_3_claim_reason_code_5',
            '2nd_payer_grp_3_dollar_amount_5',
            '2nd_payer_grp_3_adjustment_quantity_5',
            '2nd_payer_grp_3_claim_reason_code_6',
            '2nd_payer_grp_3_dollar_amount_6',
            '2nd_payer_grp_3_adjustment_quantity_6',
            '2nd_payer_grp_4_claim_group_code_4',
            '2nd_payer_grp_4_claim_reason_code_1',
            '2nd_payer_grp_4_dollar_amount_1',
            '2nd_payer_grp_4_adjustment_quantity_1',
            '2nd_payer_grp_4_claim_reason_code_2',
            '2nd_payer_grp_4_dollar_amount_2',
            '2nd_payer_grp_4_adjustment_quantity_2',
            '2nd_payer_grp_4_claim_reason_code_3',
            '2nd_payer_grp_4_dollar_amount_3',
            '2nd_payer_grp_4_adjustment_quantity_3',
            '2nd_payer_grp_4_claim_reason_code_4',
            '2nd_payer_grp_4_dollar_amount_4',
            '2nd_payer_grp_4_adjustment_quantity_4',
            '2nd_payer_grp_4_claim_reason_code_5',
            '2nd_payer_grp_4_dollar_amount_5',
            '2nd_payer_grp_4_adjustment_quantity_5',
            '2nd_payer_grp_4_claim_reason_code_6',
            '2nd_payer_grp_4_dollar_amount_6',
            '2nd_payer_grp_4_adjustment_quantity_6',
            '2nd_payer_grp_5_claim_group_code_5',
            '2nd_payer_grp_5_claim_reason_code_1',
            '2nd_payer_grp_5_dollar_amount_1',
            '2nd_payer_grp_5_adjustment_quantity_1',
            '2nd_payer_grp_5_claim_reason_code_2',
            '2nd_payer_grp_5_dollar_amount_2',
            '2nd_payer_grp_5_adjustment_quantity_2',
            '2nd_payer_grp_5_claim_reason_code_3',
            '2nd_payer_grp_5_dollar_amount_3',
            '2nd_payer_grp_5_adjustment_quantity_3',
            '2nd_payer_grp_5_claim_reason_code_4',
            '2nd_payer_grp_5_dollar_amount_4',
            '2nd_payer_grp_5_adjustment_quantity_4',
            '2nd_payer_grp_5_claim_reason_code_5',
            '2nd_payer_grp_5_dollar_amount_5',
            '2nd_payer_grp_5_adjustment_quantity_5',
            '2nd_payer_grp_5_claim_reason_code_6',
            '2nd_payer_grp_5_dollar_amount_6',
            '2nd_payer_grp_5_adjustment_quantity_6',
            '3rd_payer_grp_1_claim_group_code_1',
            '3rd_payer_grp_1_claim_reason_code_1',
            '3rd_payer_grp_1_dollar_amount_1',
            '3rd_payer_grp_1_adjustment_quantity_1',
            '3rd_payer_grp_1_claim_reason_code_2',
            '3rd_payer_grp_1_dollar_amount_2',
            '3rd_payer_grp_1_adjustment_quantity_2',
            '3rd_payer_grp_1_claim_reason_code_3',
            '3rd_payer_grp_1_dollar_amount_3',
            '3rd_payer_grp_1_adjustment_quantity_3',
            '3rd_payer_grp_1_claim_reason_code_4',
            '3rd_payer_grp_1_dollar_amount_4',
            '3rd_payer_grp_1_adjustment_quantity_4',
            '3rd_payer_grp_1_claim_reason_code_5',
            '3rd_payer_grp_1_dollar_amount_5',
            '3rd_payer_grp_1_adjustment_quantity_5',
            '3rd_payer_grp_1_claim_reason_code_6',
            '3rd_payer_grp_1_dollar_amount_6',
            '3rd_payer_grp_1_adjustment_quantity_6',
            '3rd_payer_grp_2_claim_group_code_2',
            '3rd_payer_grp_2_claim_reason_code_1',
            '3rd_payer_grp_2_dollar_amount_1',
            '3rd_payer_grp_2_adjustment_quantity_1',
            '3rd_payer_grp_2_claim_reason_code_2',
            '3rd_payer_grp_2_dollar_amount_2',
            '3rd_payer_grp_2_adjustment_quantity_2',
            '3rd_payer_grp_2_claim_reason_code_3',
            '3rd_payer_grp_2_dollar_amount_3',
            '3rd_payer_grp_2_adjustment_quantity_3',
            '3rd_payer_grp_2_claim_reason_code_4',
            '3rd_payer_grp_2_dollar_amount_4',
            '3rd_payer_grp_2_adjustment_quantity_4',
            '3rd_payer_grp_2_claim_reason_code_5',
            '3rd_payer_grp_2_dollar_amount_5',
            '3rd_payer_grp_2_adjustment_quantity_5',
            '3rd_payer_grp_2_claim_reason_code_6',
            '3rd_payer_grp_2_dollar_amount_6',
            '3rd_payer_grp_2_adjustment_quantity_6',
            '3rd_payer_grp_3_claim_group_code_3',
            '3rd_payer_grp_3_claim_reason_code_1',
            '3rd_payer_grp_3_dollar_amount_1',
            '3rd_payer_grp_3_adjustment_quantity_1',
            '3rd_payer_grp_3_claim_reason_code_2',
            '3rd_payer_grp_3_dollar_amount_2',
            '3rd_payer_grp_3_adjustment_quantity_2',
            '3rd_payer_grp_3_claim_reason_code_3',
            '3rd_payer_grp_3_dollar_amount_3',
            '3rd_payer_grp_3_adjustment_quantity_3',
            '3rd_payer_grp_3_claim_reason_code_4',
            '3rd_payer_grp_3_dollar_amount_4',
            '3rd_payer_grp_3_adjustment_quantity_4',
            '3rd_payer_grp_3_claim_reason_code_5',
            '3rd_payer_grp_3_dollar_amount_5',
            '3rd_payer_grp_3_adjustment_quantity_5',
            '3rd_payer_grp_3_claim_reason_code_6',
            '3rd_payer_grp_3_dollar_amount_6',
            '3rd_payer_grp_3_adjustment_quantity_6',
            '3rd_payer_grp_4_claim_group_code_4',
            '3rd_payer_grp_4_claim_reason_code_1',
            '3rd_payer_grp_4_dollar_amount_1',
            '3rd_payer_grp_4_adjustment_quantity_1',
            '3rd_payer_grp_4_claim_reason_code_2',
            '3rd_payer_grp_4_dollar_amount_2',
            '3rd_payer_grp_4_adjustment_quantity_2',
            '3rd_payer_grp_4_claim_reason_code_3',
            '3rd_payer_grp_4_dollar_amount_3',
            '3rd_payer_grp_4_adjustment_quantity_3',
            '3rd_payer_grp_4_claim_reason_code_4',
            '3rd_payer_grp_4_dollar_amount_4',
            '3rd_payer_grp_4_adjustment_quantity_4',
            '3rd_payer_grp_4_claim_reason_code_5',
            '3rd_payer_grp_4_dollar_amount_5',
            '3rd_payer_grp_4_adjustment_quantity_5',
            '3rd_payer_grp_4_claim_reason_code_6',
            '3rd_payer_grp_4_dollar_amount_6',
            '3rd_payer_grp_4_adjustment_quantity_6',
            '3rd_payer_grp_5_claim_group_code_5',
            '3rd_payer_grp_5_claim_reason_code_1',
            '3rd_payer_grp_5_dollar_amount_1',
            '3rd_payer_grp_5_adjustment_quantity_1',
            '3rd_payer_grp_5_claim_reason_code_2',
            '3rd_payer_grp_5_dollar_amount_2',
            '3rd_payer_grp_5_adjustment_quantity_2',
            '3rd_payer_grp_5_claim_reason_code_3',
            '3rd_payer_grp_5_dollar_amount_3',
            '3rd_payer_grp_5_adjustment_quantity_3',
            '3rd_payer_grp_5_claim_reason_code_4',
            '3rd_payer_grp_5_dollar_amount_4',
            '3rd_payer_grp_5_adjustment_quantity_4',
            '3rd_payer_grp_5_claim_reason_code_5',
            '3rd_payer_grp_5_dollar_amount_5',
            '3rd_payer_grp_5_adjustment_quantity_5',
            '3rd_payer_grp_5_claim_reason_code_6',
            '3rd_payer_grp_5_dollar_amount_6',
            '3rd_payer_grp_5_adjustment_quantity_6',
            '2nd_payer_paid_amount',
            '3rd_payer_paid_amount',
            '2nd_payer_approved_amount',
            '3rd_payer_approved_amount',
            '2nd_payer_allowed_amount',
            '3rd_payer_allowed_amount',
            '2nd_payer_name',
            '2nd_payer_primary_id',
            '3rd_payer_name',
            '3rd_payer_primary_id',
            '2nd_adjudication_payment_date',
            '3rd_adjudication_payment_date',
            '2nd_claim_adjust_ind',
            '3rd_claim_adjust',
            'primary_payer_id',
            'insured_last_name',
            'insured_first_name',
            'insured_middle_initial',
            'insured_suffix',
            'insured_address_1',
            'insured_address_2',
            'insured_city',
            'patient_last_name',
            'patient_first_name',
            'patient_middle_initial',
            'patient_suffix',
            'patient_address_1',
            'patient_address_2',
            'patient_city',
            'hvjoinkey'
        ]
    ),
    'serviceline': SourceTable(
        'csv',
        separator='|',
        trimmify_nullify=True,
        columns=[
            'record_type',
            'entity_id',
            'charge_line_number',
            'std_chg_line_hcpcs_procedure_code',
            'hcpcs_modifier_1',
            'hcpcs_modifier_2',
            'hcpcs_modifier_3',
            'hcpcs_modifier_4',
            'line_charges',
            'units_of_service',
            'anesthesia_oxygen_minutes',
            'place_of_service',
            'diagnosis_code_pointer_1',
            'diagnosis_code_pointer_2',
            'diagnosis_code_pointer_3',
            'diagnosis_code_pointer_4',
            'emergency_indicator',
            'epsdt_indicator',
            'family_planning_indicator',
            'copay_status_indicator',
            'dme_chg_line_hcpcs_procedure_code',
            'dme_rental_pur_quantity',
            'dme_rental_price',
            'dme_pur_price',
            'dme_rental_frequency_code',
            'type_of_transport',
            'transported_to_for',
            'miles',
            'purpose_of_round_trip',
            'purpose_of_stretcher',
            'patient_condition_code',
            'patient_condition_description',
            'patient_condition_description_2',
            'x_ray_availability_indicator',
            'certification_type',
            'length_of_need',
            'certification_type_code',
            'treatment_period_count',
            'arterial_blood_gas_quantity',
            'oxygen_saturation_quantity',
            'oxygen_test_condition_code',
            'oxygen_test_findings_code',
            'oxygen_test_findings_code_1',
            'oxygen_test_findings_code_2',
            'amb_condition_code_1',
            'amb_condition_code_2',
            'amb_condition_code_3',
            'amb_condition_code_4',
            'amb_condition_code_5',
            'hospice_employed_prov_ind',
            'yes_no_condition_or_response_code',
            'dme_condition_code_1',
            'dme_condition_code_2',
            'dme_condition_code_3',
            'dme_condition_code_4',
            'dme_condition_code_5',
            'service_from_date',
            'service_to_date',
            'cert_revision_date',
            'begin_therapy_date',
            'last_cert_date',
            'hgb_hct_date',
            'creatine_date',
            'oxygen_saturation_test_date',
            'blood_gas_test_date',
            'oxygen_oximetry_test_date',
            'chrio_last_x_ray_date',
            'chiro_acute_manifestation_date',
            'chiro_initial_treatment_date',
            'hcb_test_result',
            'hct_test_result',
            'epo_test_result',
            'creatine_test_result',
            'patient_height',
            'contract_type_code',
            'contract_amount',
            'contract_percentage',
            'contract_code',
            'terms_discount_percent',
            'contract_version_identifier',
            'line_item_control_number',
            'mammography_certification_number',
            'clinical_laboratory_improvement_amendment_number',
            'referring_clia_number',
            'universal_product_number_indicator',
            'universal_product_number',
            'approved_amount',
            'purchased_service_provider_identifier',
            'purchased_service_charge_amount',
            'pricing_methodology',
            'repriced_allowed_amount',
            'repriced_saving_amount',
            'repricing_organization_identifier',
            'repricing_per_diem_or_flat_rate_amount',
            'repriced_approved_ambulatory_patient_group_code',
            'repriced_approved_ambulatory_patient_group_amount',
            'product_or_service_id_qualifier',
            'procedure_code',
            'unit_or_basis_for_measurement_code',
            'repriced_approved_service_unit_count',
            'reject_reason_code',
            'policy_compliance_code',
            'exception_code',
            'ndc_code',
            'drug_unit_price',
            'quantity',
            'unit_or_basis_for_measurement_code_2',
            'prespriction_number',
            'rendering_provider_last',
            'rendering_provider_first',
            'rendering_provider_middle',
            'rendering_provider_tax_id_qual',
            'rendering_provider_primary_id',
            'rendering_provider_npi',
            'rendering_provider_specialty_code',
            'purchased_service_npi',
            'service_facility_type',
            'service_facility_name',
            'service_facility_npi',
            'service_facility_address_1',
            'service_facility_address_2',
            'service_facility_city',
            'service_facility_state',
            'service_facility_zip_code',
            'supervising_provider_last_name',
            'supervising_provider_first_name',
            'supervising_provider_mi',
            'supervising_provider_npi',
            'ordering_provider_last_name',
            'ordering_provider_first_name',
            'ordering_provider_mi',
            'ordering_provider_npi',
            'ordering_provider_address_1',
            'ordering_provider_address_2',
            'ordering_provider_city',
            'ordering_provider_state',
            'ordering_provider_zip_code',
            'referring_provider_last_name',
            'referring_provider_first_name',
            'referring_provider_mi',
            'referring_provider_npi',
            'payer_primary_id',
            'primary_paid_amount',
            'service_id_qual',
            'paid_service_unit_count',
            'line_number',
            'claim_adjustment_group_code_1',
            'adj_group_1_reason_1',
            'adj_group_1_amt_1',
            'adj_group_1_qty_1',
            'adj_group_1_reason_2',
            'adj_group_1_amt_2',
            'adj_group_1_qty_2',
            'adj_group_1_reason_3',
            'adj_group_1_amt_3',
            'adj_group_1_qty_3',
            'adj_group_1_reason_4',
            'adj_group_1_amt_4',
            'adj_group_1_qty_4',
            'adj_group_1_reason_5',
            'adj_group_1_amt_5',
            'adj_group_1_qty_5',
            'adj_group_1_reason_6',
            'adj_group_1_amt_6',
            'adj_group_1_qty_6',
            'claim_adjustment_group_code_2',
            'adj_group_2_reason_1',
            'adj_group_2_amt_1',
            'adj_group_2_qty_1',
            'adj_group_2_reason_2',
            'adj_group_2_amt_2',
            'adj_group_2_qty_2',
            'adj_group_2_reason_3',
            'adj_group_2_amt_3',
            'adj_group_2_qty_3',
            'adj_group_2_reason_4',
            'adj_group_2_amt_4',
            'adj_group_2_qty_4',
            'adj_group_2_reason_5',
            'adj_group_2_amt_5',
            'adj_group_2_qty_5',
            'adj_group_2_reason_6',
            'adj_group_2_amt_6',
            'adj_group_2_qty_6',
            'claim_adjustment_group_code_3',
            'adj_group_3_reason_1',
            'adj_group_3_amt_1',
            'adj_group_3_qty_1',
            'adj_group_3_reason_2',
            'adj_group_3_amt_2',
            'adj_group_3_qty_2',
            'adj_group_3_reason_3',
            'adj_group_3_amt_3',
            'adj_group_3_qty_3',
            'adj_group_3_reason_4',
            'adj_group_3_amt_4',
            'adj_group_3_qty_4',
            'adj_group_3_reason_5',
            'adj_group_3_amt_5',
            'adj_group_3_qty_5',
            'adj_group_3_reason_6',
            'adj_group_3_amt_6',
            'adj_group_3_qty_6',
            'claim_adjustment_group_code_4',
            'adj_group_4_reason_1',
            'adj_group_4_amt_1',
            'adj_group_4_qty_1',
            'adj_group_4_reason_2',
            'adj_group_4_amt_2',
            'adj_group_4_qty_2',
            'adj_group_4_reason_3',
            'adj_group_4_amt_3',
            'adj_group_4_qty_3',
            'adj_group_4_reason_4',
            'adj_group_4_amt_4',
            'adj_group_4_qty_4',
            'adj_group_4_reason_5',
            'adj_group_4_amt_5',
            'adj_group_4_qty_5',
            'adj_group_4_reason_6',
            'adj_group_4_amt_6',
            'adj_group_4_qty_6',
            'claim_adjustment_group_code_5',
            'adj_group_5_reason_1',
            'adj_group_5_amt_1',
            'adj_group_5_qty_1',
            'adj_group_5_reason_2',
            'adj_group_5_amt_2',
            'adj_group_5_qty_2',
            'adj_group_5_reason_3',
            'adj_group_5_amt_3',
            'adj_group_5_qty_3',
            'adj_group_5_reason_4',
            'adj_group_5amt_4',
            'adj_group_5_qty_4',
            'adj_group_5_reason_5',
            'adj_group_5_amt_5',
            'adj_group_5_qty_5',
            'adj_group_5_reason_6',
            'adj_group_5_amt_6',
            'adj_group_5_qty_6',
            'adjudication_or_payment_date',
            'hvjoinkey'
        ]
    )
}
