SELECT DISTINCT
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
    claim_type,
    date_received,
    date_service,
    date_service_end,
    inst_date_admitted,
    inst_date_discharged,
    inst_admit_type_std_id,
    inst_admit_type_vendor_id,
    inst_admit_type_vendor_desc,
    inst_admit_source_std_id,
    inst_admit_source_vendor_id,
    inst_admit_source_vendor_desc,
    inst_discharge_status_std_id,
    inst_discharge_status_vendor_id,
    inst_discharge_status_vendor_desc,
    obscure_inst_type_of_bill(inst_type_of_bill_std_id) AS inst_type_of_bill_std_id,
    inst_type_of_bill_vendor_id,
    inst_type_of_bill_vendor_desc,
    inst_drg_std_id,
    inst_drg_vendor_id,
    inst_drg_vendor_desc,
    place_of_service_std_id,
    place_of_service_vendor_id,
    place_of_service_vendor_desc,
    service_line_number,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_priority,
    admit_diagnosis_ind,
    procedure_code,
    procedure_code_qual,
    principal_proc_ind,
    procedure_units,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    revenue_code,
    ndc_code,
    medical_coverage_type,
    line_charge,
    line_allowed,
    total_charge,
    total_allowed,
    filter_due_to_inst_type_of_bill(prov_rendering_npi, inst_type_of_bill_std_id) AS prov_rendering_npi,
    filter_due_to_inst_type_of_bill(prov_billing_npi, inst_type_of_bill_std_id) AS prov_billing_npi,
    filter_due_to_inst_type_of_bill(prov_referring_npi, inst_type_of_bill_std_id) AS prov_referring_npi,
    filter_due_to_inst_type_of_bill(prov_facility_npi, inst_type_of_bill_std_id) AS prov_facility_npi,
    payer_vendor_id,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    prov_rendering_vendor_id,
    prov_rendering_tax_id,
    prov_rendering_dea_id,
    prov_rendering_ssn,
    filter_due_to_inst_type_of_bill(prov_rendering_state_license, inst_type_of_bill_std_id) AS prov_rendering_state_license,
    filter_due_to_inst_type_of_bill(prov_rendering_upin, inst_type_of_bill_std_id) AS prov_rendering_upin,
    filter_due_to_inst_type_of_bill(prov_rendering_commercial_id, inst_type_of_bill_std_id) AS prov_rendering_commercial_id,
    filter_due_to_inst_type_of_bill(prov_rendering_name_1, inst_type_of_bill_std_id) AS prov_rendering_name_1,
    filter_due_to_inst_type_of_bill(prov_rendering_name_2, inst_type_of_bill_std_id) AS prov_rendering_name_2,
    prov_rendering_address_1,
    prov_rendering_address_2,
    prov_rendering_city,
    prov_rendering_state,
    prov_rendering_zip,
    prov_rendering_std_taxonomy,
    prov_rendering_vendor_specialty,
    prov_billing_vendor_id,
    filter_due_to_inst_type_of_bill(prov_billing_tax_id, inst_type_of_bill_std_id) AS prov_billing_tax_id,
    prov_billing_dea_id,
    prov_billing_ssn,
    filter_due_to_inst_type_of_bill(prov_billing_state_license, inst_type_of_bill_std_id) AS prov_billing_state_license,
    filter_due_to_inst_type_of_bill(prov_billing_upin, inst_type_of_bill_std_id) AS prov_billing_upin,
    prov_billing_commercial_id,
    filter_due_to_inst_type_of_bill(prov_billing_name_1, inst_type_of_bill_std_id) AS prov_billing_name_1,
    filter_due_to_inst_type_of_bill(prov_billing_name_2, inst_type_of_bill_std_id) AS prov_billing_name_2,
    filter_due_to_inst_type_of_bill(prov_billing_address_1, inst_type_of_bill_std_id) AS prov_billing_address_1,
    filter_due_to_inst_type_of_bill(prov_billing_address_2, inst_type_of_bill_std_id) AS prov_billing_address_2,
    filter_due_to_inst_type_of_bill(prov_billing_city, inst_type_of_bill_std_id) AS prov_billing_city,
    filter_due_to_inst_type_of_bill(prov_billing_state, inst_type_of_bill_std_id) AS prov_billing_state,
    filter_due_to_inst_type_of_bill(prov_billing_zip, inst_type_of_bill_std_id) AS prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_billing_vendor_specialty,
    prov_referring_vendor_id,
    prov_referring_tax_id,
    prov_referring_dea_id,
    prov_referring_ssn,
    filter_due_to_inst_type_of_bill(prov_referring_state_license, inst_type_of_bill_std_id) AS prov_referring_state_license,
    filter_due_to_inst_type_of_bill(prov_referring_upin, inst_type_of_bill_std_id) AS prov_referring_upin,
    filter_due_to_inst_type_of_bill(prov_referring_commercial_id, inst_type_of_bill_std_id) AS prov_referring_commercial_id,
    filter_due_to_inst_type_of_bill(prov_referring_name_1, inst_type_of_bill_std_id) AS prov_referring_name_1,
    filter_due_to_inst_type_of_bill(prov_referring_name_2, inst_type_of_bill_std_id) AS prov_referring_name_2,
    prov_referring_address_1,
    prov_referring_address_2,
    prov_referring_city,
    prov_referring_state,
    prov_referring_zip,
    prov_referring_std_taxonomy,
    prov_referring_vendor_specialty,
    prov_facility_vendor_id,
    prov_facility_tax_id,
    prov_facility_dea_id,
    prov_facility_ssn,
    filter_due_to_inst_type_of_bill(prov_facility_state_license, inst_type_of_bill_std_id) AS prov_facility_state_license,
    prov_facility_upin,
    filter_due_to_inst_type_of_bill(prov_facility_commercial_id, inst_type_of_bill_std_id) AS prov_facility_commercial_id,
    filter_due_to_inst_type_of_bill(prov_facility_name_1, inst_type_of_bill_std_id) AS prov_facility_name_1,
    prov_facility_name_2,
    filter_due_to_inst_type_of_bill(prov_facility_address_1, inst_type_of_bill_std_id) AS prov_facility_address_1,
    filter_due_to_inst_type_of_bill(prov_facility_address_2, inst_type_of_bill_std_id) AS prov_facility_address_2,
    filter_due_to_inst_type_of_bill(prov_facility_city, inst_type_of_bill_std_id) AS prov_facility_city,
    filter_due_to_inst_type_of_bill(prov_facility_state, inst_type_of_bill_std_id) AS prov_facility_state,
    filter_due_to_inst_type_of_bill(prov_facility_zip, inst_type_of_bill_std_id) AS prov_facility_zip,
    prov_facility_std_taxonomy,
    prov_facility_vendor_specialty,
    cob_payer_vendor_id_1,
    cob_payer_seq_code_1,
    cob_payer_hpid_1,
    cob_payer_claim_filing_ind_code_1,
    cob_ins_type_code_1,
    cob_payer_vendor_id_2,
    cob_payer_seq_code_2,
    cob_payer_hpid_2,
    cob_payer_claim_filing_ind_code_2,
    cob_ins_type_code_2,
    part_provider
FROM claimremedi_08_norm_srv
