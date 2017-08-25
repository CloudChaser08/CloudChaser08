DROP VIEW IF EXISTS default.medicalclaims;
CREATE VIEW default.medicalclaims (
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
        inst_type_of_bill_std_id,
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
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
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
        prov_rendering_state_license,
        prov_rendering_upin,
        prov_rendering_commercial_id,
        prov_rendering_name_1,
        prov_rendering_name_2,
        prov_rendering_address_1,
        prov_rendering_address_2,
        prov_rendering_city,
        prov_rendering_state,
        prov_rendering_zip,
        prov_rendering_std_taxonomy,
        prov_rendering_vendor_specialty,
        prov_billing_vendor_id,
        prov_billing_tax_id,
        prov_billing_dea_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_commercial_id,
        prov_billing_name_1,
        prov_billing_name_2,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_billing_vendor_specialty,
        prov_referring_vendor_id,
        prov_referring_tax_id,
        prov_referring_dea_id,
        prov_referring_ssn,
        prov_referring_state_license,
        prov_referring_upin,
        prov_referring_commercial_id,
        prov_referring_name_1,
        prov_referring_name_2,
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
        prov_facility_state_license,
        prov_facility_upin,
        prov_facility_commercial_id,
        prov_facility_name_1,
        prov_facility_name_2,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
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
        part_provider,
        part_processdate
        )
    AS SELECT record_id,
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
    CASE
        WHEN claim_type = "P" THEN NULL
        WHEN SUBSTRING(inst_type_of_bill_std_id, 1, 1) = '3' THEN CONCAT('X', SUBSTRING(inst_type_of_bill_std_id, 2, LEN(inst_type_of_bill_std_id)-1))
        ELSE inst_type_of_bill_std_id
    END AS inst_type_of_bill_std_id,
    inst_type_of_bill_vendor_id,
    inst_type_of_bill_vendor_desc,
    inst_drg_std_id,
    inst_drg_vendor_id,
    inst_drg_vendor_desc,
    CASE
        WHEN claim_type = "I" THEN NULL
        WHEN claim_type = "P" AND inst_type_of_bill_std_id IS NOT NULL AND place_of_service_std_id IS NULL AND part_provider = "emdeon" THEN SUBSTRING(inst_type_of_bill_std_id, 1, 2)
        ELSE place_of_service_std_id
    END AS place_of_service_std_id,
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
    CASE
        WHEN prov_rendering_npi = '5' THEN NULL
        WHEN prov_rendering_npi = '05' THEN NULL
        WHEN prov_rendering_npi = '6' THEN NULL
        WHEN prov_rendering_npi = '06' THEN NULL
        WHEN prov_rendering_npi = '7' THEN NULL
        WHEN prov_rendering_npi = '07' THEN NULL
        WHEN prov_rendering_npi = '8' THEN NULL
        WHEN prov_rendering_npi = '08' THEN NULL
        WHEN prov_rendering_npi = '9' THEN NULL
        WHEN prov_rendering_npi = '09' THEN NULL
        WHEN prov_rendering_npi = '12' THEN NULL
        WHEN prov_rendering_npi = '13' THEN NULL
        WHEN prov_rendering_npi = '14' THEN NULL
        WHEN prov_rendering_npi = '33' THEN NULL
        ELSE prov_rendering_npi
    END as prov_rendering_npi,
    CASE
        WHEN prov_billing_npi = '5' THEN NULL
        WHEN prov_billing_npi = '05' THEN NULL
        WHEN prov_billing_npi = '6' THEN NULL
        WHEN prov_billing_npi = '06' THEN NULL
        WHEN prov_billing_npi = '7' THEN NULL
        WHEN prov_billing_npi = '07' THEN NULL
        WHEN prov_billing_npi = '8' THEN NULL
        WHEN prov_billing_npi = '08' THEN NULL
        WHEN prov_billing_npi = '9' THEN NULL
        WHEN prov_billing_npi = '09' THEN NULL
        WHEN prov_billing_npi = '12' THEN NULL
        WHEN prov_billing_npi = '13' THEN NULL
        WHEN prov_billing_npi = '14' THEN NULL
        WHEN prov_billing_npi = '33' THEN NULL
        ELSE prov_billing_npi
    END as prov_billing_npi,
    CASE
        WHEN prov_referring_npi = '5' THEN NULL
        WHEN prov_referring_npi = '05' THEN NULL
        WHEN prov_referring_npi = '6' THEN NULL
        WHEN prov_referring_npi = '06' THEN NULL
        WHEN prov_referring_npi = '7' THEN NULL
        WHEN prov_referring_npi = '07' THEN NULL
        WHEN prov_referring_npi = '8' THEN NULL
        WHEN prov_referring_npi = '08' THEN NULL
        WHEN prov_referring_npi = '9' THEN NULL
        WHEN prov_referring_npi = '09' THEN NULL
        WHEN prov_referring_npi = '12' THEN NULL
        WHEN prov_referring_npi = '13' THEN NULL
        WHEN prov_referring_npi = '14' THEN NULL
        WHEN prov_referring_npi = '33' THEN NULL
        ELSE prov_referring_npi
    END as prov_referring_npi,
    CASE
        WHEN prov_facility_npi = '5' THEN NULL
        WHEN prov_facility_npi = '05' THEN NULL
        WHEN prov_facility_npi = '6' THEN NULL
        WHEN prov_facility_npi = '06' THEN NULL
        WHEN prov_facility_npi = '7' THEN NULL
        WHEN prov_facility_npi = '07' THEN NULL
        WHEN prov_facility_npi = '8' THEN NULL
        WHEN prov_facility_npi = '08' THEN NULL
        WHEN prov_facility_npi = '9' THEN NULL
        WHEN prov_facility_npi = '09' THEN NULL
        WHEN prov_facility_npi = '12' THEN NULL
        WHEN prov_facility_npi = '13' THEN NULL
        WHEN prov_facility_npi = '14' THEN NULL
        WHEN prov_facility_npi = '33' THEN NULL
        ELSE prov_facility_npi
    END as prov_facility_npi,
    payer_vendor_id,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    CASE
        WHEN prov_rendering_vendor_id = '5' THEN NULL
        WHEN prov_rendering_vendor_id = '05' THEN NULL
        WHEN prov_rendering_vendor_id = '6' THEN NULL
        WHEN prov_rendering_vendor_id = '06' THEN NULL
        WHEN prov_rendering_vendor_id = '7' THEN NULL
        WHEN prov_rendering_vendor_id = '07' THEN NULL
        WHEN prov_rendering_vendor_id = '8' THEN NULL
        WHEN prov_rendering_vendor_id = '08' THEN NULL
        WHEN prov_rendering_vendor_id = '9' THEN NULL
        WHEN prov_rendering_vendor_id = '09' THEN NULL
        WHEN prov_rendering_vendor_id = '12' THEN NULL
        WHEN prov_rendering_vendor_id = '13' THEN NULL
        WHEN prov_rendering_vendor_id = '14' THEN NULL
        WHEN prov_rendering_vendor_id = '33' THEN NULL
        ELSE prov_rendering_vendor_id
    END as prov_rendering_vendor_id,
    CASE
        WHEN prov_rendering_tax_id = '5' THEN NULL
        WHEN prov_rendering_tax_id = '05' THEN NULL
        WHEN prov_rendering_tax_id = '6' THEN NULL
        WHEN prov_rendering_tax_id = '06' THEN NULL
        WHEN prov_rendering_tax_id = '7' THEN NULL
        WHEN prov_rendering_tax_id = '07' THEN NULL
        WHEN prov_rendering_tax_id = '8' THEN NULL
        WHEN prov_rendering_tax_id = '08' THEN NULL
        WHEN prov_rendering_tax_id = '9' THEN NULL
        WHEN prov_rendering_tax_id = '09' THEN NULL
        WHEN prov_rendering_tax_id = '12' THEN NULL
        WHEN prov_rendering_tax_id = '13' THEN NULL
        WHEN prov_rendering_tax_id = '14' THEN NULL
        WHEN prov_rendering_tax_id = '33' THEN NULL
        ELSE prov_rendering_tax_id
    END as prov_rendering_tax_id,
    CASE
        WHEN prov_rendering_dea_id = '5' THEN NULL
        WHEN prov_rendering_dea_id = '05' THEN NULL
        WHEN prov_rendering_dea_id = '6' THEN NULL
        WHEN prov_rendering_dea_id = '06' THEN NULL
        WHEN prov_rendering_dea_id = '7' THEN NULL
        WHEN prov_rendering_dea_id = '07' THEN NULL
        WHEN prov_rendering_dea_id = '8' THEN NULL
        WHEN prov_rendering_dea_id = '08' THEN NULL
        WHEN prov_rendering_dea_id = '9' THEN NULL
        WHEN prov_rendering_dea_id = '09' THEN NULL
        WHEN prov_rendering_dea_id = '12' THEN NULL
        WHEN prov_rendering_dea_id = '13' THEN NULL
        WHEN prov_rendering_dea_id = '14' THEN NULL
        WHEN prov_rendering_dea_id = '33' THEN NULL
        ELSE prov_rendering_dea_id
    END as prov_rendering_dea_id,
    CASE
        WHEN prov_rendering_ssn = '5' THEN NULL
        WHEN prov_rendering_ssn = '05' THEN NULL
        WHEN prov_rendering_ssn = '6' THEN NULL
        WHEN prov_rendering_ssn = '06' THEN NULL
        WHEN prov_rendering_ssn = '7' THEN NULL
        WHEN prov_rendering_ssn = '07' THEN NULL
        WHEN prov_rendering_ssn = '8' THEN NULL
        WHEN prov_rendering_ssn = '08' THEN NULL
        WHEN prov_rendering_ssn = '9' THEN NULL
        WHEN prov_rendering_ssn = '09' THEN NULL
        WHEN prov_rendering_ssn = '12' THEN NULL
        WHEN prov_rendering_ssn = '13' THEN NULL
        WHEN prov_rendering_ssn = '14' THEN NULL
        WHEN prov_rendering_ssn = '33' THEN NULL
        ELSE prov_rendering_ssn
    END as prov_rendering_ssn,
    CASE
        WHEN prov_rendering_state_license = '5' THEN NULL
        WHEN prov_rendering_state_license = '05' THEN NULL
        WHEN prov_rendering_state_license = '6' THEN NULL
        WHEN prov_rendering_state_license = '06' THEN NULL
        WHEN prov_rendering_state_license = '7' THEN NULL
        WHEN prov_rendering_state_license = '07' THEN NULL
        WHEN prov_rendering_state_license = '8' THEN NULL
        WHEN prov_rendering_state_license = '08' THEN NULL
        WHEN prov_rendering_state_license = '9' THEN NULL
        WHEN prov_rendering_state_license = '09' THEN NULL
        WHEN prov_rendering_state_license = '12' THEN NULL
        WHEN prov_rendering_state_license = '13' THEN NULL
        WHEN prov_rendering_state_license = '14' THEN NULL
        WHEN prov_rendering_state_license = '33' THEN NULL
        ELSE prov_rendering_state_license
    END as prov_rendering_state_license,
    CASE
        WHEN prov_rendering_upin = '5' THEN NULL
        WHEN prov_rendering_upin = '05' THEN NULL
        WHEN prov_rendering_upin = '6' THEN NULL
        WHEN prov_rendering_upin = '06' THEN NULL
        WHEN prov_rendering_upin = '7' THEN NULL
        WHEN prov_rendering_upin = '07' THEN NULL
        WHEN prov_rendering_upin = '8' THEN NULL
        WHEN prov_rendering_upin = '08' THEN NULL
        WHEN prov_rendering_upin = '9' THEN NULL
        WHEN prov_rendering_upin = '09' THEN NULL
        WHEN prov_rendering_upin = '12' THEN NULL
        WHEN prov_rendering_upin = '13' THEN NULL
        WHEN prov_rendering_upin = '14' THEN NULL
        WHEN prov_rendering_upin = '33' THEN NULL
        ELSE prov_rendering_upin
    END as prov_rendering_upin,
    CASE
        WHEN prov_rendering_commercial_id = '5' THEN NULL
        WHEN prov_rendering_commercial_id = '05' THEN NULL
        WHEN prov_rendering_commercial_id = '6' THEN NULL
        WHEN prov_rendering_commercial_id = '06' THEN NULL
        WHEN prov_rendering_commercial_id = '7' THEN NULL
        WHEN prov_rendering_commercial_id = '07' THEN NULL
        WHEN prov_rendering_commercial_id = '8' THEN NULL
        WHEN prov_rendering_commercial_id = '08' THEN NULL
        WHEN prov_rendering_commercial_id = '9' THEN NULL
        WHEN prov_rendering_commercial_id = '09' THEN NULL
        WHEN prov_rendering_commercial_id = '12' THEN NULL
        WHEN prov_rendering_commercial_id = '13' THEN NULL
        WHEN prov_rendering_commercial_id = '14' THEN NULL
        WHEN prov_rendering_commercial_id = '33' THEN NULL
        ELSE prov_rendering_commercial_id
    END as prov_rendering_commercial_id,
    CASE
        WHEN prov_rendering_name_1 = '5' THEN NULL
        WHEN prov_rendering_name_1 = '05' THEN NULL
        WHEN prov_rendering_name_1 = '6' THEN NULL
        WHEN prov_rendering_name_1 = '06' THEN NULL
        WHEN prov_rendering_name_1 = '7' THEN NULL
        WHEN prov_rendering_name_1 = '07' THEN NULL
        WHEN prov_rendering_name_1 = '8' THEN NULL
        WHEN prov_rendering_name_1 = '08' THEN NULL
        WHEN prov_rendering_name_1 = '9' THEN NULL
        WHEN prov_rendering_name_1 = '09' THEN NULL
        WHEN prov_rendering_name_1 = '12' THEN NULL
        WHEN prov_rendering_name_1 = '13' THEN NULL
        WHEN prov_rendering_name_1 = '14' THEN NULL
        WHEN prov_rendering_name_1 = '33' THEN NULL
        ELSE prov_rendering_name_1
    END as prov_rendering_name_1,
    CASE
        WHEN prov_rendering_name_2 = '5' THEN NULL
        WHEN prov_rendering_name_2 = '05' THEN NULL
        WHEN prov_rendering_name_2 = '6' THEN NULL
        WHEN prov_rendering_name_2 = '06' THEN NULL
        WHEN prov_rendering_name_2 = '7' THEN NULL
        WHEN prov_rendering_name_2 = '07' THEN NULL
        WHEN prov_rendering_name_2 = '8' THEN NULL
        WHEN prov_rendering_name_2 = '08' THEN NULL
        WHEN prov_rendering_name_2 = '9' THEN NULL
        WHEN prov_rendering_name_2 = '09' THEN NULL
        WHEN prov_rendering_name_2 = '12' THEN NULL
        WHEN prov_rendering_name_2 = '13' THEN NULL
        WHEN prov_rendering_name_2 = '14' THEN NULL
        WHEN prov_rendering_name_2 = '33' THEN NULL
        ELSE prov_rendering_name_2
    END as prov_rendering_name_2,
    CASE
        WHEN prov_rendering_address_1 = '5' THEN NULL
        WHEN prov_rendering_address_1 = '05' THEN NULL
        WHEN prov_rendering_address_1 = '6' THEN NULL
        WHEN prov_rendering_address_1 = '06' THEN NULL
        WHEN prov_rendering_address_1 = '7' THEN NULL
        WHEN prov_rendering_address_1 = '07' THEN NULL
        WHEN prov_rendering_address_1 = '8' THEN NULL
        WHEN prov_rendering_address_1 = '08' THEN NULL
        WHEN prov_rendering_address_1 = '9' THEN NULL
        WHEN prov_rendering_address_1 = '09' THEN NULL
        WHEN prov_rendering_address_1 = '12' THEN NULL
        WHEN prov_rendering_address_1 = '13' THEN NULL
        WHEN prov_rendering_address_1 = '14' THEN NULL
        WHEN prov_rendering_address_1 = '33' THEN NULL
        ELSE prov_rendering_address_1
    END as prov_rendering_address_1,
    CASE
        WHEN prov_rendering_address_2 = '5' THEN NULL
        WHEN prov_rendering_address_2 = '05' THEN NULL
        WHEN prov_rendering_address_2 = '6' THEN NULL
        WHEN prov_rendering_address_2 = '06' THEN NULL
        WHEN prov_rendering_address_2 = '7' THEN NULL
        WHEN prov_rendering_address_2 = '07' THEN NULL
        WHEN prov_rendering_address_2 = '8' THEN NULL
        WHEN prov_rendering_address_2 = '08' THEN NULL
        WHEN prov_rendering_address_2 = '9' THEN NULL
        WHEN prov_rendering_address_2 = '09' THEN NULL
        WHEN prov_rendering_address_2 = '12' THEN NULL
        WHEN prov_rendering_address_2 = '13' THEN NULL
        WHEN prov_rendering_address_2 = '14' THEN NULL
        WHEN prov_rendering_address_2 = '33' THEN NULL
        ELSE prov_rendering_address_2
    END as prov_rendering_address_2,
    CASE
        WHEN prov_rendering_city = '5' THEN NULL
        WHEN prov_rendering_city = '05' THEN NULL
        WHEN prov_rendering_city = '6' THEN NULL
        WHEN prov_rendering_city = '06' THEN NULL
        WHEN prov_rendering_city = '7' THEN NULL
        WHEN prov_rendering_city = '07' THEN NULL
        WHEN prov_rendering_city = '8' THEN NULL
        WHEN prov_rendering_city = '08' THEN NULL
        WHEN prov_rendering_city = '9' THEN NULL
        WHEN prov_rendering_city = '09' THEN NULL
        WHEN prov_rendering_city = '12' THEN NULL
        WHEN prov_rendering_city = '13' THEN NULL
        WHEN prov_rendering_city = '14' THEN NULL
        WHEN prov_rendering_city = '33' THEN NULL
        ELSE prov_rendering_city
    END as prov_rendering_city,
    CASE
        WHEN prov_rendering_state = '5' THEN NULL
        WHEN prov_rendering_state = '05' THEN NULL
        WHEN prov_rendering_state = '6' THEN NULL
        WHEN prov_rendering_state = '06' THEN NULL
        WHEN prov_rendering_state = '7' THEN NULL
        WHEN prov_rendering_state = '07' THEN NULL
        WHEN prov_rendering_state = '8' THEN NULL
        WHEN prov_rendering_state = '08' THEN NULL
        WHEN prov_rendering_state = '9' THEN NULL
        WHEN prov_rendering_state = '09' THEN NULL
        WHEN prov_rendering_state = '12' THEN NULL
        WHEN prov_rendering_state = '13' THEN NULL
        WHEN prov_rendering_state = '14' THEN NULL
        WHEN prov_rendering_state = '33' THEN NULL
        ELSE prov_rendering_state
    END as prov_rendering_state,
    CASE
        WHEN prov_rendering_zip = '5' THEN NULL
        WHEN prov_rendering_zip = '05' THEN NULL
        WHEN prov_rendering_zip = '6' THEN NULL
        WHEN prov_rendering_zip = '06' THEN NULL
        WHEN prov_rendering_zip = '7' THEN NULL
        WHEN prov_rendering_zip = '07' THEN NULL
        WHEN prov_rendering_zip = '8' THEN NULL
        WHEN prov_rendering_zip = '08' THEN NULL
        WHEN prov_rendering_zip = '9' THEN NULL
        WHEN prov_rendering_zip = '09' THEN NULL
        WHEN prov_rendering_zip = '12' THEN NULL
        WHEN prov_rendering_zip = '13' THEN NULL
        WHEN prov_rendering_zip = '14' THEN NULL
        WHEN prov_rendering_zip = '33' THEN NULL
        ELSE prov_rendering_zip
    END as prov_rendering_zip,
    prov_rendering_std_taxonomy,
    prov_rendering_vendor_specialty,
    CASE
        WHEN prov_billing_vendor_id = '5' THEN NULL
        WHEN prov_billing_vendor_id = '05' THEN NULL
        WHEN prov_billing_vendor_id = '6' THEN NULL
        WHEN prov_billing_vendor_id = '06' THEN NULL
        WHEN prov_billing_vendor_id = '7' THEN NULL
        WHEN prov_billing_vendor_id = '07' THEN NULL
        WHEN prov_billing_vendor_id = '8' THEN NULL
        WHEN prov_billing_vendor_id = '08' THEN NULL
        WHEN prov_billing_vendor_id = '9' THEN NULL
        WHEN prov_billing_vendor_id = '09' THEN NULL
        WHEN prov_billing_vendor_id = '12' THEN NULL
        WHEN prov_billing_vendor_id = '13' THEN NULL
        WHEN prov_billing_vendor_id = '14' THEN NULL
        WHEN prov_billing_vendor_id = '33' THEN NULL
        ELSE prov_billing_vendor_id
    END as prov_billing_vendor_id,
    CASE
        WHEN prov_billing_tax_id = '5' THEN NULL
        WHEN prov_billing_tax_id = '05' THEN NULL
        WHEN prov_billing_tax_id = '6' THEN NULL
        WHEN prov_billing_tax_id = '06' THEN NULL
        WHEN prov_billing_tax_id = '7' THEN NULL
        WHEN prov_billing_tax_id = '07' THEN NULL
        WHEN prov_billing_tax_id = '8' THEN NULL
        WHEN prov_billing_tax_id = '08' THEN NULL
        WHEN prov_billing_tax_id = '9' THEN NULL
        WHEN prov_billing_tax_id = '09' THEN NULL
        WHEN prov_billing_tax_id = '12' THEN NULL
        WHEN prov_billing_tax_id = '13' THEN NULL
        WHEN prov_billing_tax_id = '14' THEN NULL
        WHEN prov_billing_tax_id = '33' THEN NULL
        ELSE prov_billing_tax_id
    END as prov_billing_tax_id,
    CASE
        WHEN prov_billing_dea_id = '5' THEN NULL
        WHEN prov_billing_dea_id = '05' THEN NULL
        WHEN prov_billing_dea_id = '6' THEN NULL
        WHEN prov_billing_dea_id = '06' THEN NULL
        WHEN prov_billing_dea_id = '7' THEN NULL
        WHEN prov_billing_dea_id = '07' THEN NULL
        WHEN prov_billing_dea_id = '8' THEN NULL
        WHEN prov_billing_dea_id = '08' THEN NULL
        WHEN prov_billing_dea_id = '9' THEN NULL
        WHEN prov_billing_dea_id = '09' THEN NULL
        WHEN prov_billing_dea_id = '12' THEN NULL
        WHEN prov_billing_dea_id = '13' THEN NULL
        WHEN prov_billing_dea_id = '14' THEN NULL
        WHEN prov_billing_dea_id = '33' THEN NULL
        ELSE prov_billing_dea_id
    END as prov_billing_dea_id,
    CASE
        WHEN prov_billing_ssn = '5' THEN NULL
        WHEN prov_billing_ssn = '05' THEN NULL
        WHEN prov_billing_ssn = '6' THEN NULL
        WHEN prov_billing_ssn = '06' THEN NULL
        WHEN prov_billing_ssn = '7' THEN NULL
        WHEN prov_billing_ssn = '07' THEN NULL
        WHEN prov_billing_ssn = '8' THEN NULL
        WHEN prov_billing_ssn = '08' THEN NULL
        WHEN prov_billing_ssn = '9' THEN NULL
        WHEN prov_billing_ssn = '09' THEN NULL
        WHEN prov_billing_ssn = '12' THEN NULL
        WHEN prov_billing_ssn = '13' THEN NULL
        WHEN prov_billing_ssn = '14' THEN NULL
        WHEN prov_billing_ssn = '33' THEN NULL
        ELSE prov_billing_ssn
    END as prov_billing_ssn,
    CASE
        WHEN prov_billing_state_license = '5' THEN NULL
        WHEN prov_billing_state_license = '05' THEN NULL
        WHEN prov_billing_state_license = '6' THEN NULL
        WHEN prov_billing_state_license = '06' THEN NULL
        WHEN prov_billing_state_license = '7' THEN NULL
        WHEN prov_billing_state_license = '07' THEN NULL
        WHEN prov_billing_state_license = '8' THEN NULL
        WHEN prov_billing_state_license = '08' THEN NULL
        WHEN prov_billing_state_license = '9' THEN NULL
        WHEN prov_billing_state_license = '09' THEN NULL
        WHEN prov_billing_state_license = '12' THEN NULL
        WHEN prov_billing_state_license = '13' THEN NULL
        WHEN prov_billing_state_license = '14' THEN NULL
        WHEN prov_billing_state_license = '33' THEN NULL
        ELSE prov_billing_state_license
    END as prov_billing_state_license,
    CASE
        WHEN prov_billing_upin = '5' THEN NULL
        WHEN prov_billing_upin = '05' THEN NULL
        WHEN prov_billing_upin = '6' THEN NULL
        WHEN prov_billing_upin = '06' THEN NULL
        WHEN prov_billing_upin = '7' THEN NULL
        WHEN prov_billing_upin = '07' THEN NULL
        WHEN prov_billing_upin = '8' THEN NULL
        WHEN prov_billing_upin = '08' THEN NULL
        WHEN prov_billing_upin = '9' THEN NULL
        WHEN prov_billing_upin = '09' THEN NULL
        WHEN prov_billing_upin = '12' THEN NULL
        WHEN prov_billing_upin = '13' THEN NULL
        WHEN prov_billing_upin = '14' THEN NULL
        WHEN prov_billing_upin = '33' THEN NULL
        ELSE prov_billing_upin
    END as prov_billing_upin,
    CASE
        WHEN prov_billing_commercial_id = '5' THEN NULL
        WHEN prov_billing_commercial_id = '05' THEN NULL
        WHEN prov_billing_commercial_id = '6' THEN NULL
        WHEN prov_billing_commercial_id = '06' THEN NULL
        WHEN prov_billing_commercial_id = '7' THEN NULL
        WHEN prov_billing_commercial_id = '07' THEN NULL
        WHEN prov_billing_commercial_id = '8' THEN NULL
        WHEN prov_billing_commercial_id = '08' THEN NULL
        WHEN prov_billing_commercial_id = '9' THEN NULL
        WHEN prov_billing_commercial_id = '09' THEN NULL
        WHEN prov_billing_commercial_id = '12' THEN NULL
        WHEN prov_billing_commercial_id = '13' THEN NULL
        WHEN prov_billing_commercial_id = '14' THEN NULL
        WHEN prov_billing_commercial_id = '33' THEN NULL
        ELSE prov_billing_commercial_id
    END as prov_billing_commercial_id,
    CASE
        WHEN prov_billing_name_1 = '5' THEN NULL
        WHEN prov_billing_name_1 = '05' THEN NULL
        WHEN prov_billing_name_1 = '6' THEN NULL
        WHEN prov_billing_name_1 = '06' THEN NULL
        WHEN prov_billing_name_1 = '7' THEN NULL
        WHEN prov_billing_name_1 = '07' THEN NULL
        WHEN prov_billing_name_1 = '8' THEN NULL
        WHEN prov_billing_name_1 = '08' THEN NULL
        WHEN prov_billing_name_1 = '9' THEN NULL
        WHEN prov_billing_name_1 = '09' THEN NULL
        WHEN prov_billing_name_1 = '12' THEN NULL
        WHEN prov_billing_name_1 = '13' THEN NULL
        WHEN prov_billing_name_1 = '14' THEN NULL
        WHEN prov_billing_name_1 = '33' THEN NULL
        ELSE prov_billing_name_1
    END as prov_billing_name_1,
    CASE
        WHEN prov_billing_name_2 = '5' THEN NULL
        WHEN prov_billing_name_2 = '05' THEN NULL
        WHEN prov_billing_name_2 = '6' THEN NULL
        WHEN prov_billing_name_2 = '06' THEN NULL
        WHEN prov_billing_name_2 = '7' THEN NULL
        WHEN prov_billing_name_2 = '07' THEN NULL
        WHEN prov_billing_name_2 = '8' THEN NULL
        WHEN prov_billing_name_2 = '08' THEN NULL
        WHEN prov_billing_name_2 = '9' THEN NULL
        WHEN prov_billing_name_2 = '09' THEN NULL
        WHEN prov_billing_name_2 = '12' THEN NULL
        WHEN prov_billing_name_2 = '13' THEN NULL
        WHEN prov_billing_name_2 = '14' THEN NULL
        WHEN prov_billing_name_2 = '33' THEN NULL
        ELSE prov_billing_name_2
    END as prov_billing_name_2,
    CASE
        WHEN prov_billing_address_1 = '5' THEN NULL
        WHEN prov_billing_address_1 = '05' THEN NULL
        WHEN prov_billing_address_1 = '6' THEN NULL
        WHEN prov_billing_address_1 = '06' THEN NULL
        WHEN prov_billing_address_1 = '7' THEN NULL
        WHEN prov_billing_address_1 = '07' THEN NULL
        WHEN prov_billing_address_1 = '8' THEN NULL
        WHEN prov_billing_address_1 = '08' THEN NULL
        WHEN prov_billing_address_1 = '9' THEN NULL
        WHEN prov_billing_address_1 = '09' THEN NULL
        WHEN prov_billing_address_1 = '12' THEN NULL
        WHEN prov_billing_address_1 = '13' THEN NULL
        WHEN prov_billing_address_1 = '14' THEN NULL
        WHEN prov_billing_address_1 = '33' THEN NULL
        ELSE prov_billing_address_1
    END as prov_billing_address_1,
    CASE
        WHEN prov_billing_address_2 = '5' THEN NULL
        WHEN prov_billing_address_2 = '05' THEN NULL
        WHEN prov_billing_address_2 = '6' THEN NULL
        WHEN prov_billing_address_2 = '06' THEN NULL
        WHEN prov_billing_address_2 = '7' THEN NULL
        WHEN prov_billing_address_2 = '07' THEN NULL
        WHEN prov_billing_address_2 = '8' THEN NULL
        WHEN prov_billing_address_2 = '08' THEN NULL
        WHEN prov_billing_address_2 = '9' THEN NULL
        WHEN prov_billing_address_2 = '09' THEN NULL
        WHEN prov_billing_address_2 = '12' THEN NULL
        WHEN prov_billing_address_2 = '13' THEN NULL
        WHEN prov_billing_address_2 = '14' THEN NULL
        WHEN prov_billing_address_2 = '33' THEN NULL
        ELSE prov_billing_address_2
    END as prov_billing_address_2,
    CASE
        WHEN prov_billing_city = '5' THEN NULL
        WHEN prov_billing_city = '05' THEN NULL
        WHEN prov_billing_city = '6' THEN NULL
        WHEN prov_billing_city = '06' THEN NULL
        WHEN prov_billing_city = '7' THEN NULL
        WHEN prov_billing_city = '07' THEN NULL
        WHEN prov_billing_city = '8' THEN NULL
        WHEN prov_billing_city = '08' THEN NULL
        WHEN prov_billing_city = '9' THEN NULL
        WHEN prov_billing_city = '09' THEN NULL
        WHEN prov_billing_city = '12' THEN NULL
        WHEN prov_billing_city = '13' THEN NULL
        WHEN prov_billing_city = '14' THEN NULL
        WHEN prov_billing_city = '33' THEN NULL
        ELSE prov_billing_city
    END as prov_billing_city,
    CASE
        WHEN prov_billing_state = '5' THEN NULL
        WHEN prov_billing_state = '05' THEN NULL
        WHEN prov_billing_state = '6' THEN NULL
        WHEN prov_billing_state = '06' THEN NULL
        WHEN prov_billing_state = '7' THEN NULL
        WHEN prov_billing_state = '07' THEN NULL
        WHEN prov_billing_state = '8' THEN NULL
        WHEN prov_billing_state = '08' THEN NULL
        WHEN prov_billing_state = '9' THEN NULL
        WHEN prov_billing_state = '09' THEN NULL
        WHEN prov_billing_state = '12' THEN NULL
        WHEN prov_billing_state = '13' THEN NULL
        WHEN prov_billing_state = '14' THEN NULL
        WHEN prov_billing_state = '33' THEN NULL
        ELSE prov_billing_state
    END as prov_billing_state,
    CASE
        WHEN prov_billing_zip = '5' THEN NULL
        WHEN prov_billing_zip = '05' THEN NULL
        WHEN prov_billing_zip = '6' THEN NULL
        WHEN prov_billing_zip = '06' THEN NULL
        WHEN prov_billing_zip = '7' THEN NULL
        WHEN prov_billing_zip = '07' THEN NULL
        WHEN prov_billing_zip = '8' THEN NULL
        WHEN prov_billing_zip = '08' THEN NULL
        WHEN prov_billing_zip = '9' THEN NULL
        WHEN prov_billing_zip = '09' THEN NULL
        WHEN prov_billing_zip = '12' THEN NULL
        WHEN prov_billing_zip = '13' THEN NULL
        WHEN prov_billing_zip = '14' THEN NULL
        WHEN prov_billing_zip = '33' THEN NULL
        ELSE prov_billing_zip
    END as prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_billing_vendor_specialty,
    CASE
        WHEN prov_referring_vendor_id = '5' THEN NULL
        WHEN prov_referring_vendor_id = '05' THEN NULL
        WHEN prov_referring_vendor_id = '6' THEN NULL
        WHEN prov_referring_vendor_id = '06' THEN NULL
        WHEN prov_referring_vendor_id = '7' THEN NULL
        WHEN prov_referring_vendor_id = '07' THEN NULL
        WHEN prov_referring_vendor_id = '8' THEN NULL
        WHEN prov_referring_vendor_id = '08' THEN NULL
        WHEN prov_referring_vendor_id = '9' THEN NULL
        WHEN prov_referring_vendor_id = '09' THEN NULL
        WHEN prov_referring_vendor_id = '12' THEN NULL
        WHEN prov_referring_vendor_id = '13' THEN NULL
        WHEN prov_referring_vendor_id = '14' THEN NULL
        WHEN prov_referring_vendor_id = '33' THEN NULL
        ELSE prov_referring_vendor_id
    END as prov_referring_vendor_id,
    CASE
        WHEN prov_referring_tax_id = '5' THEN NULL
        WHEN prov_referring_tax_id = '05' THEN NULL
        WHEN prov_referring_tax_id = '6' THEN NULL
        WHEN prov_referring_tax_id = '06' THEN NULL
        WHEN prov_referring_tax_id = '7' THEN NULL
        WHEN prov_referring_tax_id = '07' THEN NULL
        WHEN prov_referring_tax_id = '8' THEN NULL
        WHEN prov_referring_tax_id = '08' THEN NULL
        WHEN prov_referring_tax_id = '9' THEN NULL
        WHEN prov_referring_tax_id = '09' THEN NULL
        WHEN prov_referring_tax_id = '12' THEN NULL
        WHEN prov_referring_tax_id = '13' THEN NULL
        WHEN prov_referring_tax_id = '14' THEN NULL
        WHEN prov_referring_tax_id = '33' THEN NULL
        ELSE prov_referring_tax_id
    END as prov_referring_tax_id,
    CASE
        WHEN prov_referring_dea_id = '5' THEN NULL
        WHEN prov_referring_dea_id = '05' THEN NULL
        WHEN prov_referring_dea_id = '6' THEN NULL
        WHEN prov_referring_dea_id = '06' THEN NULL
        WHEN prov_referring_dea_id = '7' THEN NULL
        WHEN prov_referring_dea_id = '07' THEN NULL
        WHEN prov_referring_dea_id = '8' THEN NULL
        WHEN prov_referring_dea_id = '08' THEN NULL
        WHEN prov_referring_dea_id = '9' THEN NULL
        WHEN prov_referring_dea_id = '09' THEN NULL
        WHEN prov_referring_dea_id = '12' THEN NULL
        WHEN prov_referring_dea_id = '13' THEN NULL
        WHEN prov_referring_dea_id = '14' THEN NULL
        WHEN prov_referring_dea_id = '33' THEN NULL
        ELSE prov_referring_dea_id
    END as prov_referring_dea_id,
    CASE
        WHEN prov_referring_ssn = '5' THEN NULL
        WHEN prov_referring_ssn = '05' THEN NULL
        WHEN prov_referring_ssn = '6' THEN NULL
        WHEN prov_referring_ssn = '06' THEN NULL
        WHEN prov_referring_ssn = '7' THEN NULL
        WHEN prov_referring_ssn = '07' THEN NULL
        WHEN prov_referring_ssn = '8' THEN NULL
        WHEN prov_referring_ssn = '08' THEN NULL
        WHEN prov_referring_ssn = '9' THEN NULL
        WHEN prov_referring_ssn = '09' THEN NULL
        WHEN prov_referring_ssn = '12' THEN NULL
        WHEN prov_referring_ssn = '13' THEN NULL
        WHEN prov_referring_ssn = '14' THEN NULL
        WHEN prov_referring_ssn = '33' THEN NULL
        ELSE prov_referring_ssn
    END as prov_referring_ssn,
    CASE
        WHEN prov_referring_state_license = '5' THEN NULL
        WHEN prov_referring_state_license = '05' THEN NULL
        WHEN prov_referring_state_license = '6' THEN NULL
        WHEN prov_referring_state_license = '06' THEN NULL
        WHEN prov_referring_state_license = '7' THEN NULL
        WHEN prov_referring_state_license = '07' THEN NULL
        WHEN prov_referring_state_license = '8' THEN NULL
        WHEN prov_referring_state_license = '08' THEN NULL
        WHEN prov_referring_state_license = '9' THEN NULL
        WHEN prov_referring_state_license = '09' THEN NULL
        WHEN prov_referring_state_license = '12' THEN NULL
        WHEN prov_referring_state_license = '13' THEN NULL
        WHEN prov_referring_state_license = '14' THEN NULL
        WHEN prov_referring_state_license = '33' THEN NULL
        ELSE prov_referring_state_license
    END as prov_referring_state_license,
    CASE
        WHEN prov_referring_upin = '5' THEN NULL
        WHEN prov_referring_upin = '05' THEN NULL
        WHEN prov_referring_upin = '6' THEN NULL
        WHEN prov_referring_upin = '06' THEN NULL
        WHEN prov_referring_upin = '7' THEN NULL
        WHEN prov_referring_upin = '07' THEN NULL
        WHEN prov_referring_upin = '8' THEN NULL
        WHEN prov_referring_upin = '08' THEN NULL
        WHEN prov_referring_upin = '9' THEN NULL
        WHEN prov_referring_upin = '09' THEN NULL
        WHEN prov_referring_upin = '12' THEN NULL
        WHEN prov_referring_upin = '13' THEN NULL
        WHEN prov_referring_upin = '14' THEN NULL
        WHEN prov_referring_upin = '33' THEN NULL
        ELSE prov_referring_upin
    END as prov_referring_upin,
    CASE
        WHEN prov_referring_commercial_id = '5' THEN NULL
        WHEN prov_referring_commercial_id = '05' THEN NULL
        WHEN prov_referring_commercial_id = '6' THEN NULL
        WHEN prov_referring_commercial_id = '06' THEN NULL
        WHEN prov_referring_commercial_id = '7' THEN NULL
        WHEN prov_referring_commercial_id = '07' THEN NULL
        WHEN prov_referring_commercial_id = '8' THEN NULL
        WHEN prov_referring_commercial_id = '08' THEN NULL
        WHEN prov_referring_commercial_id = '9' THEN NULL
        WHEN prov_referring_commercial_id = '09' THEN NULL
        WHEN prov_referring_commercial_id = '12' THEN NULL
        WHEN prov_referring_commercial_id = '13' THEN NULL
        WHEN prov_referring_commercial_id = '14' THEN NULL
        WHEN prov_referring_commercial_id = '33' THEN NULL
        ELSE prov_referring_commercial_id
    END as prov_referring_commercial_id,
    CASE
        WHEN prov_referring_name_1 = '5' THEN NULL
        WHEN prov_referring_name_1 = '05' THEN NULL
        WHEN prov_referring_name_1 = '6' THEN NULL
        WHEN prov_referring_name_1 = '06' THEN NULL
        WHEN prov_referring_name_1 = '7' THEN NULL
        WHEN prov_referring_name_1 = '07' THEN NULL
        WHEN prov_referring_name_1 = '8' THEN NULL
        WHEN prov_referring_name_1 = '08' THEN NULL
        WHEN prov_referring_name_1 = '9' THEN NULL
        WHEN prov_referring_name_1 = '09' THEN NULL
        WHEN prov_referring_name_1 = '12' THEN NULL
        WHEN prov_referring_name_1 = '13' THEN NULL
        WHEN prov_referring_name_1 = '14' THEN NULL
        WHEN prov_referring_name_1 = '33' THEN NULL
        ELSE prov_referring_name_1
    END as prov_referring_name_1,
    CASE
        WHEN prov_referring_name_2 = '5' THEN NULL
        WHEN prov_referring_name_2 = '05' THEN NULL
        WHEN prov_referring_name_2 = '6' THEN NULL
        WHEN prov_referring_name_2 = '06' THEN NULL
        WHEN prov_referring_name_2 = '7' THEN NULL
        WHEN prov_referring_name_2 = '07' THEN NULL
        WHEN prov_referring_name_2 = '8' THEN NULL
        WHEN prov_referring_name_2 = '08' THEN NULL
        WHEN prov_referring_name_2 = '9' THEN NULL
        WHEN prov_referring_name_2 = '09' THEN NULL
        WHEN prov_referring_name_2 = '12' THEN NULL
        WHEN prov_referring_name_2 = '13' THEN NULL
        WHEN prov_referring_name_2 = '14' THEN NULL
        WHEN prov_referring_name_2 = '33' THEN NULL
        ELSE prov_referring_name_2
    END as prov_referring_name_2,
    CASE
        WHEN prov_referring_address_1 = '5' THEN NULL
        WHEN prov_referring_address_1 = '05' THEN NULL
        WHEN prov_referring_address_1 = '6' THEN NULL
        WHEN prov_referring_address_1 = '06' THEN NULL
        WHEN prov_referring_address_1 = '7' THEN NULL
        WHEN prov_referring_address_1 = '07' THEN NULL
        WHEN prov_referring_address_1 = '8' THEN NULL
        WHEN prov_referring_address_1 = '08' THEN NULL
        WHEN prov_referring_address_1 = '9' THEN NULL
        WHEN prov_referring_address_1 = '09' THEN NULL
        WHEN prov_referring_address_1 = '12' THEN NULL
        WHEN prov_referring_address_1 = '13' THEN NULL
        WHEN prov_referring_address_1 = '14' THEN NULL
        WHEN prov_referring_address_1 = '33' THEN NULL
        ELSE prov_referring_address_1
    END as prov_referring_address_1,
    CASE
        WHEN prov_referring_address_2 = '5' THEN NULL
        WHEN prov_referring_address_2 = '05' THEN NULL
        WHEN prov_referring_address_2 = '6' THEN NULL
        WHEN prov_referring_address_2 = '06' THEN NULL
        WHEN prov_referring_address_2 = '7' THEN NULL
        WHEN prov_referring_address_2 = '07' THEN NULL
        WHEN prov_referring_address_2 = '8' THEN NULL
        WHEN prov_referring_address_2 = '08' THEN NULL
        WHEN prov_referring_address_2 = '9' THEN NULL
        WHEN prov_referring_address_2 = '09' THEN NULL
        WHEN prov_referring_address_2 = '12' THEN NULL
        WHEN prov_referring_address_2 = '13' THEN NULL
        WHEN prov_referring_address_2 = '14' THEN NULL
        WHEN prov_referring_address_2 = '33' THEN NULL
        ELSE prov_referring_address_2
    END as prov_referring_address_2,
    CASE
        WHEN prov_referring_city = '5' THEN NULL
        WHEN prov_referring_city = '05' THEN NULL
        WHEN prov_referring_city = '6' THEN NULL
        WHEN prov_referring_city = '06' THEN NULL
        WHEN prov_referring_city = '7' THEN NULL
        WHEN prov_referring_city = '07' THEN NULL
        WHEN prov_referring_city = '8' THEN NULL
        WHEN prov_referring_city = '08' THEN NULL
        WHEN prov_referring_city = '9' THEN NULL
        WHEN prov_referring_city = '09' THEN NULL
        WHEN prov_referring_city = '12' THEN NULL
        WHEN prov_referring_city = '13' THEN NULL
        WHEN prov_referring_city = '14' THEN NULL
        WHEN prov_referring_city = '33' THEN NULL
        ELSE prov_referring_city
    END as prov_referring_city,
    CASE
        WHEN prov_referring_state = '5' THEN NULL
        WHEN prov_referring_state = '05' THEN NULL
        WHEN prov_referring_state = '6' THEN NULL
        WHEN prov_referring_state = '06' THEN NULL
        WHEN prov_referring_state = '7' THEN NULL
        WHEN prov_referring_state = '07' THEN NULL
        WHEN prov_referring_state = '8' THEN NULL
        WHEN prov_referring_state = '08' THEN NULL
        WHEN prov_referring_state = '9' THEN NULL
        WHEN prov_referring_state = '09' THEN NULL
        WHEN prov_referring_state = '12' THEN NULL
        WHEN prov_referring_state = '13' THEN NULL
        WHEN prov_referring_state = '14' THEN NULL
        WHEN prov_referring_state = '33' THEN NULL
        ELSE prov_referring_state
    END as prov_referring_state,
    CASE
        WHEN prov_referring_zip = '5' THEN NULL
        WHEN prov_referring_zip = '05' THEN NULL
        WHEN prov_referring_zip = '6' THEN NULL
        WHEN prov_referring_zip = '06' THEN NULL
        WHEN prov_referring_zip = '7' THEN NULL
        WHEN prov_referring_zip = '07' THEN NULL
        WHEN prov_referring_zip = '8' THEN NULL
        WHEN prov_referring_zip = '08' THEN NULL
        WHEN prov_referring_zip = '9' THEN NULL
        WHEN prov_referring_zip = '09' THEN NULL
        WHEN prov_referring_zip = '12' THEN NULL
        WHEN prov_referring_zip = '13' THEN NULL
        WHEN prov_referring_zip = '14' THEN NULL
        WHEN prov_referring_zip = '33' THEN NULL
        ELSE prov_referring_zip
    END as prov_referring_zip,
    prov_referring_std_taxonomy,
    prov_referring_vendor_specialty,
    CASE
        WHEN prov_facility_vendor_id = '5' THEN NULL
        WHEN prov_facility_vendor_id = '05' THEN NULL
        WHEN prov_facility_vendor_id = '6' THEN NULL
        WHEN prov_facility_vendor_id = '06' THEN NULL
        WHEN prov_facility_vendor_id = '7' THEN NULL
        WHEN prov_facility_vendor_id = '07' THEN NULL
        WHEN prov_facility_vendor_id = '8' THEN NULL
        WHEN prov_facility_vendor_id = '08' THEN NULL
        WHEN prov_facility_vendor_id = '9' THEN NULL
        WHEN prov_facility_vendor_id = '09' THEN NULL
        WHEN prov_facility_vendor_id = '12' THEN NULL
        WHEN prov_facility_vendor_id = '13' THEN NULL
        WHEN prov_facility_vendor_id = '14' THEN NULL
        WHEN prov_facility_vendor_id = '33' THEN NULL
        ELSE prov_facility_vendor_id
    END as prov_facility_vendor_id,
    CASE
        WHEN prov_facility_tax_id = '5' THEN NULL
        WHEN prov_facility_tax_id = '05' THEN NULL
        WHEN prov_facility_tax_id = '6' THEN NULL
        WHEN prov_facility_tax_id = '06' THEN NULL
        WHEN prov_facility_tax_id = '7' THEN NULL
        WHEN prov_facility_tax_id = '07' THEN NULL
        WHEN prov_facility_tax_id = '8' THEN NULL
        WHEN prov_facility_tax_id = '08' THEN NULL
        WHEN prov_facility_tax_id = '9' THEN NULL
        WHEN prov_facility_tax_id = '09' THEN NULL
        WHEN prov_facility_tax_id = '12' THEN NULL
        WHEN prov_facility_tax_id = '13' THEN NULL
        WHEN prov_facility_tax_id = '14' THEN NULL
        WHEN prov_facility_tax_id = '33' THEN NULL
        ELSE prov_facility_tax_id
    END as prov_facility_tax_id,
    CASE
        WHEN prov_facility_dea_id = '5' THEN NULL
        WHEN prov_facility_dea_id = '05' THEN NULL
        WHEN prov_facility_dea_id = '6' THEN NULL
        WHEN prov_facility_dea_id = '06' THEN NULL
        WHEN prov_facility_dea_id = '7' THEN NULL
        WHEN prov_facility_dea_id = '07' THEN NULL
        WHEN prov_facility_dea_id = '8' THEN NULL
        WHEN prov_facility_dea_id = '08' THEN NULL
        WHEN prov_facility_dea_id = '9' THEN NULL
        WHEN prov_facility_dea_id = '09' THEN NULL
        WHEN prov_facility_dea_id = '12' THEN NULL
        WHEN prov_facility_dea_id = '13' THEN NULL
        WHEN prov_facility_dea_id = '14' THEN NULL
        WHEN prov_facility_dea_id = '33' THEN NULL
        ELSE prov_facility_dea_id
    END as prov_facility_dea_id,
    CASE
        WHEN prov_facility_ssn = '5' THEN NULL
        WHEN prov_facility_ssn = '05' THEN NULL
        WHEN prov_facility_ssn = '6' THEN NULL
        WHEN prov_facility_ssn = '06' THEN NULL
        WHEN prov_facility_ssn = '7' THEN NULL
        WHEN prov_facility_ssn = '07' THEN NULL
        WHEN prov_facility_ssn = '8' THEN NULL
        WHEN prov_facility_ssn = '08' THEN NULL
        WHEN prov_facility_ssn = '9' THEN NULL
        WHEN prov_facility_ssn = '09' THEN NULL
        WHEN prov_facility_ssn = '12' THEN NULL
        WHEN prov_facility_ssn = '13' THEN NULL
        WHEN prov_facility_ssn = '14' THEN NULL
        WHEN prov_facility_ssn = '33' THEN NULL
        ELSE prov_facility_ssn
    END as prov_facility_ssn,
    CASE
        WHEN prov_facility_state_license = '5' THEN NULL
        WHEN prov_facility_state_license = '05' THEN NULL
        WHEN prov_facility_state_license = '6' THEN NULL
        WHEN prov_facility_state_license = '06' THEN NULL
        WHEN prov_facility_state_license = '7' THEN NULL
        WHEN prov_facility_state_license = '07' THEN NULL
        WHEN prov_facility_state_license = '8' THEN NULL
        WHEN prov_facility_state_license = '08' THEN NULL
        WHEN prov_facility_state_license = '9' THEN NULL
        WHEN prov_facility_state_license = '09' THEN NULL
        WHEN prov_facility_state_license = '12' THEN NULL
        WHEN prov_facility_state_license = '13' THEN NULL
        WHEN prov_facility_state_license = '14' THEN NULL
        WHEN prov_facility_state_license = '33' THEN NULL
        ELSE prov_facility_state_license
    END as prov_facility_state_license,
    CASE
        WHEN prov_facility_upin = '5' THEN NULL
        WHEN prov_facility_upin = '05' THEN NULL
        WHEN prov_facility_upin = '6' THEN NULL
        WHEN prov_facility_upin = '06' THEN NULL
        WHEN prov_facility_upin = '7' THEN NULL
        WHEN prov_facility_upin = '07' THEN NULL
        WHEN prov_facility_upin = '8' THEN NULL
        WHEN prov_facility_upin = '08' THEN NULL
        WHEN prov_facility_upin = '9' THEN NULL
        WHEN prov_facility_upin = '09' THEN NULL
        WHEN prov_facility_upin = '12' THEN NULL
        WHEN prov_facility_upin = '13' THEN NULL
        WHEN prov_facility_upin = '14' THEN NULL
        WHEN prov_facility_upin = '33' THEN NULL
        ELSE prov_facility_upin
    END as prov_facility_upin,
    CASE
        WHEN prov_facility_commercial_id = '5' THEN NULL
        WHEN prov_facility_commercial_id = '05' THEN NULL
        WHEN prov_facility_commercial_id = '6' THEN NULL
        WHEN prov_facility_commercial_id = '06' THEN NULL
        WHEN prov_facility_commercial_id = '7' THEN NULL
        WHEN prov_facility_commercial_id = '07' THEN NULL
        WHEN prov_facility_commercial_id = '8' THEN NULL
        WHEN prov_facility_commercial_id = '08' THEN NULL
        WHEN prov_facility_commercial_id = '9' THEN NULL
        WHEN prov_facility_commercial_id = '09' THEN NULL
        WHEN prov_facility_commercial_id = '12' THEN NULL
        WHEN prov_facility_commercial_id = '13' THEN NULL
        WHEN prov_facility_commercial_id = '14' THEN NULL
        WHEN prov_facility_commercial_id = '33' THEN NULL
        ELSE prov_facility_commercial_id
    END as prov_facility_commercial_id,
    CASE
        WHEN prov_facility_name_1 = '5' THEN NULL
        WHEN prov_facility_name_1 = '05' THEN NULL
        WHEN prov_facility_name_1 = '6' THEN NULL
        WHEN prov_facility_name_1 = '06' THEN NULL
        WHEN prov_facility_name_1 = '7' THEN NULL
        WHEN prov_facility_name_1 = '07' THEN NULL
        WHEN prov_facility_name_1 = '8' THEN NULL
        WHEN prov_facility_name_1 = '08' THEN NULL
        WHEN prov_facility_name_1 = '9' THEN NULL
        WHEN prov_facility_name_1 = '09' THEN NULL
        WHEN prov_facility_name_1 = '12' THEN NULL
        WHEN prov_facility_name_1 = '13' THEN NULL
        WHEN prov_facility_name_1 = '14' THEN NULL
        WHEN prov_facility_name_1 = '33' THEN NULL
        ELSE prov_facility_name_1
    END as prov_facility_name_1,
    CASE
        WHEN prov_facility_name_2 = '5' THEN NULL
        WHEN prov_facility_name_2 = '05' THEN NULL
        WHEN prov_facility_name_2 = '6' THEN NULL
        WHEN prov_facility_name_2 = '06' THEN NULL
        WHEN prov_facility_name_2 = '7' THEN NULL
        WHEN prov_facility_name_2 = '07' THEN NULL
        WHEN prov_facility_name_2 = '8' THEN NULL
        WHEN prov_facility_name_2 = '08' THEN NULL
        WHEN prov_facility_name_2 = '9' THEN NULL
        WHEN prov_facility_name_2 = '09' THEN NULL
        WHEN prov_facility_name_2 = '12' THEN NULL
        WHEN prov_facility_name_2 = '13' THEN NULL
        WHEN prov_facility_name_2 = '14' THEN NULL
        WHEN prov_facility_name_2 = '33' THEN NULL
        ELSE prov_facility_name_2
    END as prov_facility_name_2,
    CASE
        WHEN prov_facility_address_1 = '5' THEN NULL
        WHEN prov_facility_address_1 = '05' THEN NULL
        WHEN prov_facility_address_1 = '6' THEN NULL
        WHEN prov_facility_address_1 = '06' THEN NULL
        WHEN prov_facility_address_1 = '7' THEN NULL
        WHEN prov_facility_address_1 = '07' THEN NULL
        WHEN prov_facility_address_1 = '8' THEN NULL
        WHEN prov_facility_address_1 = '08' THEN NULL
        WHEN prov_facility_address_1 = '9' THEN NULL
        WHEN prov_facility_address_1 = '09' THEN NULL
        WHEN prov_facility_address_1 = '12' THEN NULL
        WHEN prov_facility_address_1 = '13' THEN NULL
        WHEN prov_facility_address_1 = '14' THEN NULL
        WHEN prov_facility_address_1 = '33' THEN NULL
        ELSE prov_facility_address_1
    END as prov_facility_address_1,
    CASE
        WHEN prov_facility_address_2 = '5' THEN NULL
        WHEN prov_facility_address_2 = '05' THEN NULL
        WHEN prov_facility_address_2 = '6' THEN NULL
        WHEN prov_facility_address_2 = '06' THEN NULL
        WHEN prov_facility_address_2 = '7' THEN NULL
        WHEN prov_facility_address_2 = '07' THEN NULL
        WHEN prov_facility_address_2 = '8' THEN NULL
        WHEN prov_facility_address_2 = '08' THEN NULL
        WHEN prov_facility_address_2 = '9' THEN NULL
        WHEN prov_facility_address_2 = '09' THEN NULL
        WHEN prov_facility_address_2 = '12' THEN NULL
        WHEN prov_facility_address_2 = '13' THEN NULL
        WHEN prov_facility_address_2 = '14' THEN NULL
        WHEN prov_facility_address_2 = '33' THEN NULL
        ELSE prov_facility_address_2
    END as prov_facility_address_2,
    CASE
        WHEN prov_facility_city = '5' THEN NULL
        WHEN prov_facility_city = '05' THEN NULL
        WHEN prov_facility_city = '6' THEN NULL
        WHEN prov_facility_city = '06' THEN NULL
        WHEN prov_facility_city = '7' THEN NULL
        WHEN prov_facility_city = '07' THEN NULL
        WHEN prov_facility_city = '8' THEN NULL
        WHEN prov_facility_city = '08' THEN NULL
        WHEN prov_facility_city = '9' THEN NULL
        WHEN prov_facility_city = '09' THEN NULL
        WHEN prov_facility_city = '12' THEN NULL
        WHEN prov_facility_city = '13' THEN NULL
        WHEN prov_facility_city = '14' THEN NULL
        WHEN prov_facility_city = '33' THEN NULL
        ELSE prov_facility_city
    END as prov_facility_city,
    CASE
        WHEN prov_facility_state = '5' THEN NULL
        WHEN prov_facility_state = '05' THEN NULL
        WHEN prov_facility_state = '6' THEN NULL
        WHEN prov_facility_state = '06' THEN NULL
        WHEN prov_facility_state = '7' THEN NULL
        WHEN prov_facility_state = '07' THEN NULL
        WHEN prov_facility_state = '8' THEN NULL
        WHEN prov_facility_state = '08' THEN NULL
        WHEN prov_facility_state = '9' THEN NULL
        WHEN prov_facility_state = '09' THEN NULL
        WHEN prov_facility_state = '12' THEN NULL
        WHEN prov_facility_state = '13' THEN NULL
        WHEN prov_facility_state = '14' THEN NULL
        WHEN prov_facility_state = '33' THEN NULL
        ELSE prov_facility_state
    END as prov_facility_state,
    CASE
        WHEN prov_facility_zip = '5' THEN NULL
        WHEN prov_facility_zip = '05' THEN NULL
        WHEN prov_facility_zip = '6' THEN NULL
        WHEN prov_facility_zip = '06' THEN NULL
        WHEN prov_facility_zip = '7' THEN NULL
        WHEN prov_facility_zip = '07' THEN NULL
        WHEN prov_facility_zip = '8' THEN NULL
        WHEN prov_facility_zip = '08' THEN NULL
        WHEN prov_facility_zip = '9' THEN NULL
        WHEN prov_facility_zip = '09' THEN NULL
        WHEN prov_facility_zip = '12' THEN NULL
        WHEN prov_facility_zip = '13' THEN NULL
        WHEN prov_facility_zip = '14' THEN NULL
        WHEN prov_facility_zip = '33' THEN NULL
        ELSE prov_facility_zip
    END as prov_facility_zip,
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
    part_provider,
    CASE WHEN part_best_date NOT IN ('NULL', '0_PREDATES_HVM_HISTORY')
    THEN CONCAT(REGEXP_REPLACE(part_best_date, '-', '/'), '/01')
    ELSE '0_PREDATES_HVM_HISTORY'
    END AS part_processdate
FROM default.medicalclaims_new
WHERE part_provider IN ('practice_insight', 'emdeon')
UNION ALL
SELECT CAST(record_id AS bigint),
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
    CAST(date_received AS date),
    CAST(date_service AS date),
    CAST(date_service_end AS date),
    CAST(inst_date_admitted AS date),
    CAST(inst_date_discharged AS date),
    inst_admit_type_std_id,
    inst_admit_type_vendor_id,
    inst_admit_type_vendor_desc,
    inst_admit_source_std_id,
    inst_admit_source_vendor_id,
    inst_admit_source_vendor_desc,
    inst_discharge_status_std_id,
    inst_discharge_status_vendor_id,
    inst_discharge_status_vendor_desc,
    CASE
        WHEN claim_type = "P" THEN NULL
        WHEN SUBSTRING(inst_type_of_bill_std_id, 1, 1) = '3' THEN CONCAT('X', SUBSTRING(inst_type_of_bill_std_id, 2, LEN(inst_type_of_bill_std_id)-1))
        ELSE inst_type_of_bill_std_id
    END AS inst_type_of_bill_std_id,
    inst_type_of_bill_vendor_id,
    inst_type_of_bill_vendor_desc,
    inst_drg_std_id,
    inst_drg_vendor_id,
    inst_drg_vendor_desc,
    CASE
        WHEN claim_type = "I" THEN NULL
        ELSE place_of_service_std_id
    END AS place_of_service_std_id,
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
    CAST(procedure_units AS float),
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    revenue_code,
    ndc_code,
    medical_coverage_type,
    CAST(line_charge AS float),
    CAST(line_allowed AS float),
    CAST(total_charge AS float),
    CAST(total_allowed AS float),
    prov_rendering_npi,
    prov_billing_npi,
    prov_referring_npi,
    prov_facility_npi,
    payer_vendor_id,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    CASE
        WHEN prov_rendering_vendor_id = '5' THEN NULL
        WHEN prov_rendering_vendor_id = '05' THEN NULL
        WHEN prov_rendering_vendor_id = '6' THEN NULL
        WHEN prov_rendering_vendor_id = '06' THEN NULL
        WHEN prov_rendering_vendor_id = '7' THEN NULL
        WHEN prov_rendering_vendor_id = '07' THEN NULL
        WHEN prov_rendering_vendor_id = '8' THEN NULL
        WHEN prov_rendering_vendor_id = '08' THEN NULL
        WHEN prov_rendering_vendor_id = '9' THEN NULL
        WHEN prov_rendering_vendor_id = '09' THEN NULL
        WHEN prov_rendering_vendor_id = '12' THEN NULL
        WHEN prov_rendering_vendor_id = '13' THEN NULL
        WHEN prov_rendering_vendor_id = '14' THEN NULL
        WHEN prov_rendering_vendor_id = '33' THEN NULL
        ELSE prov_rendering_vendor_id
    END as prov_rendering_vendor_id,
    CASE
        WHEN prov_rendering_tax_id = '5' THEN NULL
        WHEN prov_rendering_tax_id = '05' THEN NULL
        WHEN prov_rendering_tax_id = '6' THEN NULL
        WHEN prov_rendering_tax_id = '06' THEN NULL
        WHEN prov_rendering_tax_id = '7' THEN NULL
        WHEN prov_rendering_tax_id = '07' THEN NULL
        WHEN prov_rendering_tax_id = '8' THEN NULL
        WHEN prov_rendering_tax_id = '08' THEN NULL
        WHEN prov_rendering_tax_id = '9' THEN NULL
        WHEN prov_rendering_tax_id = '09' THEN NULL
        WHEN prov_rendering_tax_id = '12' THEN NULL
        WHEN prov_rendering_tax_id = '13' THEN NULL
        WHEN prov_rendering_tax_id = '14' THEN NULL
        WHEN prov_rendering_tax_id = '33' THEN NULL
        ELSE prov_rendering_tax_id
    END as prov_rendering_tax_id,
    CASE
        WHEN prov_rendering_dea_id = '5' THEN NULL
        WHEN prov_rendering_dea_id = '05' THEN NULL
        WHEN prov_rendering_dea_id = '6' THEN NULL
        WHEN prov_rendering_dea_id = '06' THEN NULL
        WHEN prov_rendering_dea_id = '7' THEN NULL
        WHEN prov_rendering_dea_id = '07' THEN NULL
        WHEN prov_rendering_dea_id = '8' THEN NULL
        WHEN prov_rendering_dea_id = '08' THEN NULL
        WHEN prov_rendering_dea_id = '9' THEN NULL
        WHEN prov_rendering_dea_id = '09' THEN NULL
        WHEN prov_rendering_dea_id = '12' THEN NULL
        WHEN prov_rendering_dea_id = '13' THEN NULL
        WHEN prov_rendering_dea_id = '14' THEN NULL
        WHEN prov_rendering_dea_id = '33' THEN NULL
        ELSE prov_rendering_dea_id
    END as prov_rendering_dea_id,
    CASE
        WHEN prov_rendering_ssn = '5' THEN NULL
        WHEN prov_rendering_ssn = '05' THEN NULL
        WHEN prov_rendering_ssn = '6' THEN NULL
        WHEN prov_rendering_ssn = '06' THEN NULL
        WHEN prov_rendering_ssn = '7' THEN NULL
        WHEN prov_rendering_ssn = '07' THEN NULL
        WHEN prov_rendering_ssn = '8' THEN NULL
        WHEN prov_rendering_ssn = '08' THEN NULL
        WHEN prov_rendering_ssn = '9' THEN NULL
        WHEN prov_rendering_ssn = '09' THEN NULL
        WHEN prov_rendering_ssn = '12' THEN NULL
        WHEN prov_rendering_ssn = '13' THEN NULL
        WHEN prov_rendering_ssn = '14' THEN NULL
        WHEN prov_rendering_ssn = '33' THEN NULL
        ELSE prov_rendering_ssn
    END as prov_rendering_ssn,
    CASE
        WHEN prov_rendering_state_license = '5' THEN NULL
        WHEN prov_rendering_state_license = '05' THEN NULL
        WHEN prov_rendering_state_license = '6' THEN NULL
        WHEN prov_rendering_state_license = '06' THEN NULL
        WHEN prov_rendering_state_license = '7' THEN NULL
        WHEN prov_rendering_state_license = '07' THEN NULL
        WHEN prov_rendering_state_license = '8' THEN NULL
        WHEN prov_rendering_state_license = '08' THEN NULL
        WHEN prov_rendering_state_license = '9' THEN NULL
        WHEN prov_rendering_state_license = '09' THEN NULL
        WHEN prov_rendering_state_license = '12' THEN NULL
        WHEN prov_rendering_state_license = '13' THEN NULL
        WHEN prov_rendering_state_license = '14' THEN NULL
        WHEN prov_rendering_state_license = '33' THEN NULL
        ELSE prov_rendering_state_license
    END as prov_rendering_state_license,
    CASE
        WHEN prov_rendering_upin = '5' THEN NULL
        WHEN prov_rendering_upin = '05' THEN NULL
        WHEN prov_rendering_upin = '6' THEN NULL
        WHEN prov_rendering_upin = '06' THEN NULL
        WHEN prov_rendering_upin = '7' THEN NULL
        WHEN prov_rendering_upin = '07' THEN NULL
        WHEN prov_rendering_upin = '8' THEN NULL
        WHEN prov_rendering_upin = '08' THEN NULL
        WHEN prov_rendering_upin = '9' THEN NULL
        WHEN prov_rendering_upin = '09' THEN NULL
        WHEN prov_rendering_upin = '12' THEN NULL
        WHEN prov_rendering_upin = '13' THEN NULL
        WHEN prov_rendering_upin = '14' THEN NULL
        WHEN prov_rendering_upin = '33' THEN NULL
        ELSE prov_rendering_upin
    END as prov_rendering_upin,
    CASE
        WHEN prov_rendering_commercial_id = '5' THEN NULL
        WHEN prov_rendering_commercial_id = '05' THEN NULL
        WHEN prov_rendering_commercial_id = '6' THEN NULL
        WHEN prov_rendering_commercial_id = '06' THEN NULL
        WHEN prov_rendering_commercial_id = '7' THEN NULL
        WHEN prov_rendering_commercial_id = '07' THEN NULL
        WHEN prov_rendering_commercial_id = '8' THEN NULL
        WHEN prov_rendering_commercial_id = '08' THEN NULL
        WHEN prov_rendering_commercial_id = '9' THEN NULL
        WHEN prov_rendering_commercial_id = '09' THEN NULL
        WHEN prov_rendering_commercial_id = '12' THEN NULL
        WHEN prov_rendering_commercial_id = '13' THEN NULL
        WHEN prov_rendering_commercial_id = '14' THEN NULL
        WHEN prov_rendering_commercial_id = '33' THEN NULL
        ELSE prov_rendering_commercial_id
    END as prov_rendering_commercial_id,
    CASE
        WHEN prov_rendering_name_1 = '5' THEN NULL
        WHEN prov_rendering_name_1 = '05' THEN NULL
        WHEN prov_rendering_name_1 = '6' THEN NULL
        WHEN prov_rendering_name_1 = '06' THEN NULL
        WHEN prov_rendering_name_1 = '7' THEN NULL
        WHEN prov_rendering_name_1 = '07' THEN NULL
        WHEN prov_rendering_name_1 = '8' THEN NULL
        WHEN prov_rendering_name_1 = '08' THEN NULL
        WHEN prov_rendering_name_1 = '9' THEN NULL
        WHEN prov_rendering_name_1 = '09' THEN NULL
        WHEN prov_rendering_name_1 = '12' THEN NULL
        WHEN prov_rendering_name_1 = '13' THEN NULL
        WHEN prov_rendering_name_1 = '14' THEN NULL
        WHEN prov_rendering_name_1 = '33' THEN NULL
        ELSE prov_rendering_name_1
    END as prov_rendering_name_1,
    CASE
        WHEN prov_rendering_name_2 = '5' THEN NULL
        WHEN prov_rendering_name_2 = '05' THEN NULL
        WHEN prov_rendering_name_2 = '6' THEN NULL
        WHEN prov_rendering_name_2 = '06' THEN NULL
        WHEN prov_rendering_name_2 = '7' THEN NULL
        WHEN prov_rendering_name_2 = '07' THEN NULL
        WHEN prov_rendering_name_2 = '8' THEN NULL
        WHEN prov_rendering_name_2 = '08' THEN NULL
        WHEN prov_rendering_name_2 = '9' THEN NULL
        WHEN prov_rendering_name_2 = '09' THEN NULL
        WHEN prov_rendering_name_2 = '12' THEN NULL
        WHEN prov_rendering_name_2 = '13' THEN NULL
        WHEN prov_rendering_name_2 = '14' THEN NULL
        WHEN prov_rendering_name_2 = '33' THEN NULL
        ELSE prov_rendering_name_2
    END as prov_rendering_name_2,
    CASE
        WHEN prov_rendering_address_1 = '5' THEN NULL
        WHEN prov_rendering_address_1 = '05' THEN NULL
        WHEN prov_rendering_address_1 = '6' THEN NULL
        WHEN prov_rendering_address_1 = '06' THEN NULL
        WHEN prov_rendering_address_1 = '7' THEN NULL
        WHEN prov_rendering_address_1 = '07' THEN NULL
        WHEN prov_rendering_address_1 = '8' THEN NULL
        WHEN prov_rendering_address_1 = '08' THEN NULL
        WHEN prov_rendering_address_1 = '9' THEN NULL
        WHEN prov_rendering_address_1 = '09' THEN NULL
        WHEN prov_rendering_address_1 = '12' THEN NULL
        WHEN prov_rendering_address_1 = '13' THEN NULL
        WHEN prov_rendering_address_1 = '14' THEN NULL
        WHEN prov_rendering_address_1 = '33' THEN NULL
        ELSE prov_rendering_address_1
    END as prov_rendering_address_1,
    CASE
        WHEN prov_rendering_address_2 = '5' THEN NULL
        WHEN prov_rendering_address_2 = '05' THEN NULL
        WHEN prov_rendering_address_2 = '6' THEN NULL
        WHEN prov_rendering_address_2 = '06' THEN NULL
        WHEN prov_rendering_address_2 = '7' THEN NULL
        WHEN prov_rendering_address_2 = '07' THEN NULL
        WHEN prov_rendering_address_2 = '8' THEN NULL
        WHEN prov_rendering_address_2 = '08' THEN NULL
        WHEN prov_rendering_address_2 = '9' THEN NULL
        WHEN prov_rendering_address_2 = '09' THEN NULL
        WHEN prov_rendering_address_2 = '12' THEN NULL
        WHEN prov_rendering_address_2 = '13' THEN NULL
        WHEN prov_rendering_address_2 = '14' THEN NULL
        WHEN prov_rendering_address_2 = '33' THEN NULL
        ELSE prov_rendering_address_2
    END as prov_rendering_address_2,
    CASE
        WHEN prov_rendering_city = '5' THEN NULL
        WHEN prov_rendering_city = '05' THEN NULL
        WHEN prov_rendering_city = '6' THEN NULL
        WHEN prov_rendering_city = '06' THEN NULL
        WHEN prov_rendering_city = '7' THEN NULL
        WHEN prov_rendering_city = '07' THEN NULL
        WHEN prov_rendering_city = '8' THEN NULL
        WHEN prov_rendering_city = '08' THEN NULL
        WHEN prov_rendering_city = '9' THEN NULL
        WHEN prov_rendering_city = '09' THEN NULL
        WHEN prov_rendering_city = '12' THEN NULL
        WHEN prov_rendering_city = '13' THEN NULL
        WHEN prov_rendering_city = '14' THEN NULL
        WHEN prov_rendering_city = '33' THEN NULL
        ELSE prov_rendering_city
    END as prov_rendering_city,
    CASE
        WHEN prov_rendering_state = '5' THEN NULL
        WHEN prov_rendering_state = '05' THEN NULL
        WHEN prov_rendering_state = '6' THEN NULL
        WHEN prov_rendering_state = '06' THEN NULL
        WHEN prov_rendering_state = '7' THEN NULL
        WHEN prov_rendering_state = '07' THEN NULL
        WHEN prov_rendering_state = '8' THEN NULL
        WHEN prov_rendering_state = '08' THEN NULL
        WHEN prov_rendering_state = '9' THEN NULL
        WHEN prov_rendering_state = '09' THEN NULL
        WHEN prov_rendering_state = '12' THEN NULL
        WHEN prov_rendering_state = '13' THEN NULL
        WHEN prov_rendering_state = '14' THEN NULL
        WHEN prov_rendering_state = '33' THEN NULL
        ELSE prov_rendering_state
    END as prov_rendering_state,
    CASE
        WHEN prov_rendering_zip = '5' THEN NULL
        WHEN prov_rendering_zip = '05' THEN NULL
        WHEN prov_rendering_zip = '6' THEN NULL
        WHEN prov_rendering_zip = '06' THEN NULL
        WHEN prov_rendering_zip = '7' THEN NULL
        WHEN prov_rendering_zip = '07' THEN NULL
        WHEN prov_rendering_zip = '8' THEN NULL
        WHEN prov_rendering_zip = '08' THEN NULL
        WHEN prov_rendering_zip = '9' THEN NULL
        WHEN prov_rendering_zip = '09' THEN NULL
        WHEN prov_rendering_zip = '12' THEN NULL
        WHEN prov_rendering_zip = '13' THEN NULL
        WHEN prov_rendering_zip = '14' THEN NULL
        WHEN prov_rendering_zip = '33' THEN NULL
        ELSE prov_rendering_zip
    END as prov_rendering_zip,
    prov_rendering_std_taxonomy,
    prov_rendering_vendor_specialty,
    CASE
        WHEN prov_billing_vendor_id = '5' THEN NULL
        WHEN prov_billing_vendor_id = '05' THEN NULL
        WHEN prov_billing_vendor_id = '6' THEN NULL
        WHEN prov_billing_vendor_id = '06' THEN NULL
        WHEN prov_billing_vendor_id = '7' THEN NULL
        WHEN prov_billing_vendor_id = '07' THEN NULL
        WHEN prov_billing_vendor_id = '8' THEN NULL
        WHEN prov_billing_vendor_id = '08' THEN NULL
        WHEN prov_billing_vendor_id = '9' THEN NULL
        WHEN prov_billing_vendor_id = '09' THEN NULL
        WHEN prov_billing_vendor_id = '12' THEN NULL
        WHEN prov_billing_vendor_id = '13' THEN NULL
        WHEN prov_billing_vendor_id = '14' THEN NULL
        WHEN prov_billing_vendor_id = '33' THEN NULL
        ELSE prov_billing_vendor_id
    END as prov_billing_vendor_id,
    CASE
        WHEN prov_billing_tax_id = '5' THEN NULL
        WHEN prov_billing_tax_id = '05' THEN NULL
        WHEN prov_billing_tax_id = '6' THEN NULL
        WHEN prov_billing_tax_id = '06' THEN NULL
        WHEN prov_billing_tax_id = '7' THEN NULL
        WHEN prov_billing_tax_id = '07' THEN NULL
        WHEN prov_billing_tax_id = '8' THEN NULL
        WHEN prov_billing_tax_id = '08' THEN NULL
        WHEN prov_billing_tax_id = '9' THEN NULL
        WHEN prov_billing_tax_id = '09' THEN NULL
        WHEN prov_billing_tax_id = '12' THEN NULL
        WHEN prov_billing_tax_id = '13' THEN NULL
        WHEN prov_billing_tax_id = '14' THEN NULL
        WHEN prov_billing_tax_id = '33' THEN NULL
        ELSE prov_billing_tax_id
    END as prov_billing_tax_id,
    CASE
        WHEN prov_billing_dea_id = '5' THEN NULL
        WHEN prov_billing_dea_id = '05' THEN NULL
        WHEN prov_billing_dea_id = '6' THEN NULL
        WHEN prov_billing_dea_id = '06' THEN NULL
        WHEN prov_billing_dea_id = '7' THEN NULL
        WHEN prov_billing_dea_id = '07' THEN NULL
        WHEN prov_billing_dea_id = '8' THEN NULL
        WHEN prov_billing_dea_id = '08' THEN NULL
        WHEN prov_billing_dea_id = '9' THEN NULL
        WHEN prov_billing_dea_id = '09' THEN NULL
        WHEN prov_billing_dea_id = '12' THEN NULL
        WHEN prov_billing_dea_id = '13' THEN NULL
        WHEN prov_billing_dea_id = '14' THEN NULL
        WHEN prov_billing_dea_id = '33' THEN NULL
        ELSE prov_billing_dea_id
    END as prov_billing_dea_id,
    CASE
        WHEN prov_billing_ssn = '5' THEN NULL
        WHEN prov_billing_ssn = '05' THEN NULL
        WHEN prov_billing_ssn = '6' THEN NULL
        WHEN prov_billing_ssn = '06' THEN NULL
        WHEN prov_billing_ssn = '7' THEN NULL
        WHEN prov_billing_ssn = '07' THEN NULL
        WHEN prov_billing_ssn = '8' THEN NULL
        WHEN prov_billing_ssn = '08' THEN NULL
        WHEN prov_billing_ssn = '9' THEN NULL
        WHEN prov_billing_ssn = '09' THEN NULL
        WHEN prov_billing_ssn = '12' THEN NULL
        WHEN prov_billing_ssn = '13' THEN NULL
        WHEN prov_billing_ssn = '14' THEN NULL
        WHEN prov_billing_ssn = '33' THEN NULL
        ELSE prov_billing_ssn
    END as prov_billing_ssn,
    CASE
        WHEN prov_billing_state_license = '5' THEN NULL
        WHEN prov_billing_state_license = '05' THEN NULL
        WHEN prov_billing_state_license = '6' THEN NULL
        WHEN prov_billing_state_license = '06' THEN NULL
        WHEN prov_billing_state_license = '7' THEN NULL
        WHEN prov_billing_state_license = '07' THEN NULL
        WHEN prov_billing_state_license = '8' THEN NULL
        WHEN prov_billing_state_license = '08' THEN NULL
        WHEN prov_billing_state_license = '9' THEN NULL
        WHEN prov_billing_state_license = '09' THEN NULL
        WHEN prov_billing_state_license = '12' THEN NULL
        WHEN prov_billing_state_license = '13' THEN NULL
        WHEN prov_billing_state_license = '14' THEN NULL
        WHEN prov_billing_state_license = '33' THEN NULL
        ELSE prov_billing_state_license
    END as prov_billing_state_license,
    CASE
        WHEN prov_billing_upin = '5' THEN NULL
        WHEN prov_billing_upin = '05' THEN NULL
        WHEN prov_billing_upin = '6' THEN NULL
        WHEN prov_billing_upin = '06' THEN NULL
        WHEN prov_billing_upin = '7' THEN NULL
        WHEN prov_billing_upin = '07' THEN NULL
        WHEN prov_billing_upin = '8' THEN NULL
        WHEN prov_billing_upin = '08' THEN NULL
        WHEN prov_billing_upin = '9' THEN NULL
        WHEN prov_billing_upin = '09' THEN NULL
        WHEN prov_billing_upin = '12' THEN NULL
        WHEN prov_billing_upin = '13' THEN NULL
        WHEN prov_billing_upin = '14' THEN NULL
        WHEN prov_billing_upin = '33' THEN NULL
        ELSE prov_billing_upin
    END as prov_billing_upin,
    CASE
        WHEN prov_billing_commercial_id = '5' THEN NULL
        WHEN prov_billing_commercial_id = '05' THEN NULL
        WHEN prov_billing_commercial_id = '6' THEN NULL
        WHEN prov_billing_commercial_id = '06' THEN NULL
        WHEN prov_billing_commercial_id = '7' THEN NULL
        WHEN prov_billing_commercial_id = '07' THEN NULL
        WHEN prov_billing_commercial_id = '8' THEN NULL
        WHEN prov_billing_commercial_id = '08' THEN NULL
        WHEN prov_billing_commercial_id = '9' THEN NULL
        WHEN prov_billing_commercial_id = '09' THEN NULL
        WHEN prov_billing_commercial_id = '12' THEN NULL
        WHEN prov_billing_commercial_id = '13' THEN NULL
        WHEN prov_billing_commercial_id = '14' THEN NULL
        WHEN prov_billing_commercial_id = '33' THEN NULL
        ELSE prov_billing_commercial_id
    END as prov_billing_commercial_id,
    CASE
        WHEN prov_billing_name_1 = '5' THEN NULL
        WHEN prov_billing_name_1 = '05' THEN NULL
        WHEN prov_billing_name_1 = '6' THEN NULL
        WHEN prov_billing_name_1 = '06' THEN NULL
        WHEN prov_billing_name_1 = '7' THEN NULL
        WHEN prov_billing_name_1 = '07' THEN NULL
        WHEN prov_billing_name_1 = '8' THEN NULL
        WHEN prov_billing_name_1 = '08' THEN NULL
        WHEN prov_billing_name_1 = '9' THEN NULL
        WHEN prov_billing_name_1 = '09' THEN NULL
        WHEN prov_billing_name_1 = '12' THEN NULL
        WHEN prov_billing_name_1 = '13' THEN NULL
        WHEN prov_billing_name_1 = '14' THEN NULL
        WHEN prov_billing_name_1 = '33' THEN NULL
        ELSE prov_billing_name_1
    END as prov_billing_name_1,
    CASE
        WHEN prov_billing_name_2 = '5' THEN NULL
        WHEN prov_billing_name_2 = '05' THEN NULL
        WHEN prov_billing_name_2 = '6' THEN NULL
        WHEN prov_billing_name_2 = '06' THEN NULL
        WHEN prov_billing_name_2 = '7' THEN NULL
        WHEN prov_billing_name_2 = '07' THEN NULL
        WHEN prov_billing_name_2 = '8' THEN NULL
        WHEN prov_billing_name_2 = '08' THEN NULL
        WHEN prov_billing_name_2 = '9' THEN NULL
        WHEN prov_billing_name_2 = '09' THEN NULL
        WHEN prov_billing_name_2 = '12' THEN NULL
        WHEN prov_billing_name_2 = '13' THEN NULL
        WHEN prov_billing_name_2 = '14' THEN NULL
        WHEN prov_billing_name_2 = '33' THEN NULL
        ELSE prov_billing_name_2
    END as prov_billing_name_2,
    CASE
        WHEN prov_billing_address_1 = '5' THEN NULL
        WHEN prov_billing_address_1 = '05' THEN NULL
        WHEN prov_billing_address_1 = '6' THEN NULL
        WHEN prov_billing_address_1 = '06' THEN NULL
        WHEN prov_billing_address_1 = '7' THEN NULL
        WHEN prov_billing_address_1 = '07' THEN NULL
        WHEN prov_billing_address_1 = '8' THEN NULL
        WHEN prov_billing_address_1 = '08' THEN NULL
        WHEN prov_billing_address_1 = '9' THEN NULL
        WHEN prov_billing_address_1 = '09' THEN NULL
        WHEN prov_billing_address_1 = '12' THEN NULL
        WHEN prov_billing_address_1 = '13' THEN NULL
        WHEN prov_billing_address_1 = '14' THEN NULL
        WHEN prov_billing_address_1 = '33' THEN NULL
        ELSE prov_billing_address_1
    END as prov_billing_address_1,
    CASE
        WHEN prov_billing_address_2 = '5' THEN NULL
        WHEN prov_billing_address_2 = '05' THEN NULL
        WHEN prov_billing_address_2 = '6' THEN NULL
        WHEN prov_billing_address_2 = '06' THEN NULL
        WHEN prov_billing_address_2 = '7' THEN NULL
        WHEN prov_billing_address_2 = '07' THEN NULL
        WHEN prov_billing_address_2 = '8' THEN NULL
        WHEN prov_billing_address_2 = '08' THEN NULL
        WHEN prov_billing_address_2 = '9' THEN NULL
        WHEN prov_billing_address_2 = '09' THEN NULL
        WHEN prov_billing_address_2 = '12' THEN NULL
        WHEN prov_billing_address_2 = '13' THEN NULL
        WHEN prov_billing_address_2 = '14' THEN NULL
        WHEN prov_billing_address_2 = '33' THEN NULL
        ELSE prov_billing_address_2
    END as prov_billing_address_2,
    CASE
        WHEN prov_billing_city = '5' THEN NULL
        WHEN prov_billing_city = '05' THEN NULL
        WHEN prov_billing_city = '6' THEN NULL
        WHEN prov_billing_city = '06' THEN NULL
        WHEN prov_billing_city = '7' THEN NULL
        WHEN prov_billing_city = '07' THEN NULL
        WHEN prov_billing_city = '8' THEN NULL
        WHEN prov_billing_city = '08' THEN NULL
        WHEN prov_billing_city = '9' THEN NULL
        WHEN prov_billing_city = '09' THEN NULL
        WHEN prov_billing_city = '12' THEN NULL
        WHEN prov_billing_city = '13' THEN NULL
        WHEN prov_billing_city = '14' THEN NULL
        WHEN prov_billing_city = '33' THEN NULL
        ELSE prov_billing_city
    END as prov_billing_city,
    CASE
        WHEN prov_billing_state = '5' THEN NULL
        WHEN prov_billing_state = '05' THEN NULL
        WHEN prov_billing_state = '6' THEN NULL
        WHEN prov_billing_state = '06' THEN NULL
        WHEN prov_billing_state = '7' THEN NULL
        WHEN prov_billing_state = '07' THEN NULL
        WHEN prov_billing_state = '8' THEN NULL
        WHEN prov_billing_state = '08' THEN NULL
        WHEN prov_billing_state = '9' THEN NULL
        WHEN prov_billing_state = '09' THEN NULL
        WHEN prov_billing_state = '12' THEN NULL
        WHEN prov_billing_state = '13' THEN NULL
        WHEN prov_billing_state = '14' THEN NULL
        WHEN prov_billing_state = '33' THEN NULL
        ELSE prov_billing_state
    END as prov_billing_state,
    CASE
        WHEN prov_billing_zip = '5' THEN NULL
        WHEN prov_billing_zip = '05' THEN NULL
        WHEN prov_billing_zip = '6' THEN NULL
        WHEN prov_billing_zip = '06' THEN NULL
        WHEN prov_billing_zip = '7' THEN NULL
        WHEN prov_billing_zip = '07' THEN NULL
        WHEN prov_billing_zip = '8' THEN NULL
        WHEN prov_billing_zip = '08' THEN NULL
        WHEN prov_billing_zip = '9' THEN NULL
        WHEN prov_billing_zip = '09' THEN NULL
        WHEN prov_billing_zip = '12' THEN NULL
        WHEN prov_billing_zip = '13' THEN NULL
        WHEN prov_billing_zip = '14' THEN NULL
        WHEN prov_billing_zip = '33' THEN NULL
        ELSE prov_billing_zip
    END as prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_billing_vendor_specialty,
    CASE
        WHEN prov_referring_vendor_id = '5' THEN NULL
        WHEN prov_referring_vendor_id = '05' THEN NULL
        WHEN prov_referring_vendor_id = '6' THEN NULL
        WHEN prov_referring_vendor_id = '06' THEN NULL
        WHEN prov_referring_vendor_id = '7' THEN NULL
        WHEN prov_referring_vendor_id = '07' THEN NULL
        WHEN prov_referring_vendor_id = '8' THEN NULL
        WHEN prov_referring_vendor_id = '08' THEN NULL
        WHEN prov_referring_vendor_id = '9' THEN NULL
        WHEN prov_referring_vendor_id = '09' THEN NULL
        WHEN prov_referring_vendor_id = '12' THEN NULL
        WHEN prov_referring_vendor_id = '13' THEN NULL
        WHEN prov_referring_vendor_id = '14' THEN NULL
        WHEN prov_referring_vendor_id = '33' THEN NULL
        ELSE prov_referring_vendor_id
    END as prov_referring_vendor_id,
    CASE
        WHEN prov_referring_tax_id = '5' THEN NULL
        WHEN prov_referring_tax_id = '05' THEN NULL
        WHEN prov_referring_tax_id = '6' THEN NULL
        WHEN prov_referring_tax_id = '06' THEN NULL
        WHEN prov_referring_tax_id = '7' THEN NULL
        WHEN prov_referring_tax_id = '07' THEN NULL
        WHEN prov_referring_tax_id = '8' THEN NULL
        WHEN prov_referring_tax_id = '08' THEN NULL
        WHEN prov_referring_tax_id = '9' THEN NULL
        WHEN prov_referring_tax_id = '09' THEN NULL
        WHEN prov_referring_tax_id = '12' THEN NULL
        WHEN prov_referring_tax_id = '13' THEN NULL
        WHEN prov_referring_tax_id = '14' THEN NULL
        WHEN prov_referring_tax_id = '33' THEN NULL
        ELSE prov_referring_tax_id
    END as prov_referring_tax_id,
    CASE
        WHEN prov_referring_dea_id = '5' THEN NULL
        WHEN prov_referring_dea_id = '05' THEN NULL
        WHEN prov_referring_dea_id = '6' THEN NULL
        WHEN prov_referring_dea_id = '06' THEN NULL
        WHEN prov_referring_dea_id = '7' THEN NULL
        WHEN prov_referring_dea_id = '07' THEN NULL
        WHEN prov_referring_dea_id = '8' THEN NULL
        WHEN prov_referring_dea_id = '08' THEN NULL
        WHEN prov_referring_dea_id = '9' THEN NULL
        WHEN prov_referring_dea_id = '09' THEN NULL
        WHEN prov_referring_dea_id = '12' THEN NULL
        WHEN prov_referring_dea_id = '13' THEN NULL
        WHEN prov_referring_dea_id = '14' THEN NULL
        WHEN prov_referring_dea_id = '33' THEN NULL
        ELSE prov_referring_dea_id
    END as prov_referring_dea_id,
    CASE
        WHEN prov_referring_ssn = '5' THEN NULL
        WHEN prov_referring_ssn = '05' THEN NULL
        WHEN prov_referring_ssn = '6' THEN NULL
        WHEN prov_referring_ssn = '06' THEN NULL
        WHEN prov_referring_ssn = '7' THEN NULL
        WHEN prov_referring_ssn = '07' THEN NULL
        WHEN prov_referring_ssn = '8' THEN NULL
        WHEN prov_referring_ssn = '08' THEN NULL
        WHEN prov_referring_ssn = '9' THEN NULL
        WHEN prov_referring_ssn = '09' THEN NULL
        WHEN prov_referring_ssn = '12' THEN NULL
        WHEN prov_referring_ssn = '13' THEN NULL
        WHEN prov_referring_ssn = '14' THEN NULL
        WHEN prov_referring_ssn = '33' THEN NULL
        ELSE prov_referring_ssn
    END as prov_referring_ssn,
    CASE
        WHEN prov_referring_state_license = '5' THEN NULL
        WHEN prov_referring_state_license = '05' THEN NULL
        WHEN prov_referring_state_license = '6' THEN NULL
        WHEN prov_referring_state_license = '06' THEN NULL
        WHEN prov_referring_state_license = '7' THEN NULL
        WHEN prov_referring_state_license = '07' THEN NULL
        WHEN prov_referring_state_license = '8' THEN NULL
        WHEN prov_referring_state_license = '08' THEN NULL
        WHEN prov_referring_state_license = '9' THEN NULL
        WHEN prov_referring_state_license = '09' THEN NULL
        WHEN prov_referring_state_license = '12' THEN NULL
        WHEN prov_referring_state_license = '13' THEN NULL
        WHEN prov_referring_state_license = '14' THEN NULL
        WHEN prov_referring_state_license = '33' THEN NULL
        ELSE prov_referring_state_license
    END as prov_referring_state_license,
    CASE
        WHEN prov_referring_upin = '5' THEN NULL
        WHEN prov_referring_upin = '05' THEN NULL
        WHEN prov_referring_upin = '6' THEN NULL
        WHEN prov_referring_upin = '06' THEN NULL
        WHEN prov_referring_upin = '7' THEN NULL
        WHEN prov_referring_upin = '07' THEN NULL
        WHEN prov_referring_upin = '8' THEN NULL
        WHEN prov_referring_upin = '08' THEN NULL
        WHEN prov_referring_upin = '9' THEN NULL
        WHEN prov_referring_upin = '09' THEN NULL
        WHEN prov_referring_upin = '12' THEN NULL
        WHEN prov_referring_upin = '13' THEN NULL
        WHEN prov_referring_upin = '14' THEN NULL
        WHEN prov_referring_upin = '33' THEN NULL
        ELSE prov_referring_upin
    END as prov_referring_upin,
    CASE
        WHEN prov_referring_commercial_id = '5' THEN NULL
        WHEN prov_referring_commercial_id = '05' THEN NULL
        WHEN prov_referring_commercial_id = '6' THEN NULL
        WHEN prov_referring_commercial_id = '06' THEN NULL
        WHEN prov_referring_commercial_id = '7' THEN NULL
        WHEN prov_referring_commercial_id = '07' THEN NULL
        WHEN prov_referring_commercial_id = '8' THEN NULL
        WHEN prov_referring_commercial_id = '08' THEN NULL
        WHEN prov_referring_commercial_id = '9' THEN NULL
        WHEN prov_referring_commercial_id = '09' THEN NULL
        WHEN prov_referring_commercial_id = '12' THEN NULL
        WHEN prov_referring_commercial_id = '13' THEN NULL
        WHEN prov_referring_commercial_id = '14' THEN NULL
        WHEN prov_referring_commercial_id = '33' THEN NULL
        ELSE prov_referring_commercial_id
    END as prov_referring_commercial_id,
    CASE
        WHEN prov_referring_name_1 = '5' THEN NULL
        WHEN prov_referring_name_1 = '05' THEN NULL
        WHEN prov_referring_name_1 = '6' THEN NULL
        WHEN prov_referring_name_1 = '06' THEN NULL
        WHEN prov_referring_name_1 = '7' THEN NULL
        WHEN prov_referring_name_1 = '07' THEN NULL
        WHEN prov_referring_name_1 = '8' THEN NULL
        WHEN prov_referring_name_1 = '08' THEN NULL
        WHEN prov_referring_name_1 = '9' THEN NULL
        WHEN prov_referring_name_1 = '09' THEN NULL
        WHEN prov_referring_name_1 = '12' THEN NULL
        WHEN prov_referring_name_1 = '13' THEN NULL
        WHEN prov_referring_name_1 = '14' THEN NULL
        WHEN prov_referring_name_1 = '33' THEN NULL
        ELSE prov_referring_name_1
    END as prov_referring_name_1,
    CASE
        WHEN prov_referring_name_2 = '5' THEN NULL
        WHEN prov_referring_name_2 = '05' THEN NULL
        WHEN prov_referring_name_2 = '6' THEN NULL
        WHEN prov_referring_name_2 = '06' THEN NULL
        WHEN prov_referring_name_2 = '7' THEN NULL
        WHEN prov_referring_name_2 = '07' THEN NULL
        WHEN prov_referring_name_2 = '8' THEN NULL
        WHEN prov_referring_name_2 = '08' THEN NULL
        WHEN prov_referring_name_2 = '9' THEN NULL
        WHEN prov_referring_name_2 = '09' THEN NULL
        WHEN prov_referring_name_2 = '12' THEN NULL
        WHEN prov_referring_name_2 = '13' THEN NULL
        WHEN prov_referring_name_2 = '14' THEN NULL
        WHEN prov_referring_name_2 = '33' THEN NULL
        ELSE prov_referring_name_2
    END as prov_referring_name_2,
    CASE
        WHEN prov_referring_address_1 = '5' THEN NULL
        WHEN prov_referring_address_1 = '05' THEN NULL
        WHEN prov_referring_address_1 = '6' THEN NULL
        WHEN prov_referring_address_1 = '06' THEN NULL
        WHEN prov_referring_address_1 = '7' THEN NULL
        WHEN prov_referring_address_1 = '07' THEN NULL
        WHEN prov_referring_address_1 = '8' THEN NULL
        WHEN prov_referring_address_1 = '08' THEN NULL
        WHEN prov_referring_address_1 = '9' THEN NULL
        WHEN prov_referring_address_1 = '09' THEN NULL
        WHEN prov_referring_address_1 = '12' THEN NULL
        WHEN prov_referring_address_1 = '13' THEN NULL
        WHEN prov_referring_address_1 = '14' THEN NULL
        WHEN prov_referring_address_1 = '33' THEN NULL
        ELSE prov_referring_address_1
    END as prov_referring_address_1,
    CASE
        WHEN prov_referring_address_2 = '5' THEN NULL
        WHEN prov_referring_address_2 = '05' THEN NULL
        WHEN prov_referring_address_2 = '6' THEN NULL
        WHEN prov_referring_address_2 = '06' THEN NULL
        WHEN prov_referring_address_2 = '7' THEN NULL
        WHEN prov_referring_address_2 = '07' THEN NULL
        WHEN prov_referring_address_2 = '8' THEN NULL
        WHEN prov_referring_address_2 = '08' THEN NULL
        WHEN prov_referring_address_2 = '9' THEN NULL
        WHEN prov_referring_address_2 = '09' THEN NULL
        WHEN prov_referring_address_2 = '12' THEN NULL
        WHEN prov_referring_address_2 = '13' THEN NULL
        WHEN prov_referring_address_2 = '14' THEN NULL
        WHEN prov_referring_address_2 = '33' THEN NULL
        ELSE prov_referring_address_2
    END as prov_referring_address_2,
    CASE
        WHEN prov_referring_city = '5' THEN NULL
        WHEN prov_referring_city = '05' THEN NULL
        WHEN prov_referring_city = '6' THEN NULL
        WHEN prov_referring_city = '06' THEN NULL
        WHEN prov_referring_city = '7' THEN NULL
        WHEN prov_referring_city = '07' THEN NULL
        WHEN prov_referring_city = '8' THEN NULL
        WHEN prov_referring_city = '08' THEN NULL
        WHEN prov_referring_city = '9' THEN NULL
        WHEN prov_referring_city = '09' THEN NULL
        WHEN prov_referring_city = '12' THEN NULL
        WHEN prov_referring_city = '13' THEN NULL
        WHEN prov_referring_city = '14' THEN NULL
        WHEN prov_referring_city = '33' THEN NULL
        ELSE prov_referring_city
    END as prov_referring_city,
    CASE
        WHEN prov_referring_state = '5' THEN NULL
        WHEN prov_referring_state = '05' THEN NULL
        WHEN prov_referring_state = '6' THEN NULL
        WHEN prov_referring_state = '06' THEN NULL
        WHEN prov_referring_state = '7' THEN NULL
        WHEN prov_referring_state = '07' THEN NULL
        WHEN prov_referring_state = '8' THEN NULL
        WHEN prov_referring_state = '08' THEN NULL
        WHEN prov_referring_state = '9' THEN NULL
        WHEN prov_referring_state = '09' THEN NULL
        WHEN prov_referring_state = '12' THEN NULL
        WHEN prov_referring_state = '13' THEN NULL
        WHEN prov_referring_state = '14' THEN NULL
        WHEN prov_referring_state = '33' THEN NULL
        ELSE prov_referring_state
    END as prov_referring_state,
    CASE
        WHEN prov_referring_zip = '5' THEN NULL
        WHEN prov_referring_zip = '05' THEN NULL
        WHEN prov_referring_zip = '6' THEN NULL
        WHEN prov_referring_zip = '06' THEN NULL
        WHEN prov_referring_zip = '7' THEN NULL
        WHEN prov_referring_zip = '07' THEN NULL
        WHEN prov_referring_zip = '8' THEN NULL
        WHEN prov_referring_zip = '08' THEN NULL
        WHEN prov_referring_zip = '9' THEN NULL
        WHEN prov_referring_zip = '09' THEN NULL
        WHEN prov_referring_zip = '12' THEN NULL
        WHEN prov_referring_zip = '13' THEN NULL
        WHEN prov_referring_zip = '14' THEN NULL
        WHEN prov_referring_zip = '33' THEN NULL
        ELSE prov_referring_zip
    END as prov_referring_zip,
    prov_referring_std_taxonomy,
    prov_referring_vendor_specialty,
    CASE
        WHEN prov_facility_vendor_id = '5' THEN NULL
        WHEN prov_facility_vendor_id = '05' THEN NULL
        WHEN prov_facility_vendor_id = '6' THEN NULL
        WHEN prov_facility_vendor_id = '06' THEN NULL
        WHEN prov_facility_vendor_id = '7' THEN NULL
        WHEN prov_facility_vendor_id = '07' THEN NULL
        WHEN prov_facility_vendor_id = '8' THEN NULL
        WHEN prov_facility_vendor_id = '08' THEN NULL
        WHEN prov_facility_vendor_id = '9' THEN NULL
        WHEN prov_facility_vendor_id = '09' THEN NULL
        WHEN prov_facility_vendor_id = '12' THEN NULL
        WHEN prov_facility_vendor_id = '13' THEN NULL
        WHEN prov_facility_vendor_id = '14' THEN NULL
        WHEN prov_facility_vendor_id = '33' THEN NULL
        ELSE prov_facility_vendor_id
    END as prov_facility_vendor_id,
    CASE
        WHEN prov_facility_tax_id = '5' THEN NULL
        WHEN prov_facility_tax_id = '05' THEN NULL
        WHEN prov_facility_tax_id = '6' THEN NULL
        WHEN prov_facility_tax_id = '06' THEN NULL
        WHEN prov_facility_tax_id = '7' THEN NULL
        WHEN prov_facility_tax_id = '07' THEN NULL
        WHEN prov_facility_tax_id = '8' THEN NULL
        WHEN prov_facility_tax_id = '08' THEN NULL
        WHEN prov_facility_tax_id = '9' THEN NULL
        WHEN prov_facility_tax_id = '09' THEN NULL
        WHEN prov_facility_tax_id = '12' THEN NULL
        WHEN prov_facility_tax_id = '13' THEN NULL
        WHEN prov_facility_tax_id = '14' THEN NULL
        WHEN prov_facility_tax_id = '33' THEN NULL
        ELSE prov_facility_tax_id
    END as prov_facility_tax_id,
    CASE
        WHEN prov_facility_dea_id = '5' THEN NULL
        WHEN prov_facility_dea_id = '05' THEN NULL
        WHEN prov_facility_dea_id = '6' THEN NULL
        WHEN prov_facility_dea_id = '06' THEN NULL
        WHEN prov_facility_dea_id = '7' THEN NULL
        WHEN prov_facility_dea_id = '07' THEN NULL
        WHEN prov_facility_dea_id = '8' THEN NULL
        WHEN prov_facility_dea_id = '08' THEN NULL
        WHEN prov_facility_dea_id = '9' THEN NULL
        WHEN prov_facility_dea_id = '09' THEN NULL
        WHEN prov_facility_dea_id = '12' THEN NULL
        WHEN prov_facility_dea_id = '13' THEN NULL
        WHEN prov_facility_dea_id = '14' THEN NULL
        WHEN prov_facility_dea_id = '33' THEN NULL
        ELSE prov_facility_dea_id
    END as prov_facility_dea_id,
    CASE
        WHEN prov_facility_ssn = '5' THEN NULL
        WHEN prov_facility_ssn = '05' THEN NULL
        WHEN prov_facility_ssn = '6' THEN NULL
        WHEN prov_facility_ssn = '06' THEN NULL
        WHEN prov_facility_ssn = '7' THEN NULL
        WHEN prov_facility_ssn = '07' THEN NULL
        WHEN prov_facility_ssn = '8' THEN NULL
        WHEN prov_facility_ssn = '08' THEN NULL
        WHEN prov_facility_ssn = '9' THEN NULL
        WHEN prov_facility_ssn = '09' THEN NULL
        WHEN prov_facility_ssn = '12' THEN NULL
        WHEN prov_facility_ssn = '13' THEN NULL
        WHEN prov_facility_ssn = '14' THEN NULL
        WHEN prov_facility_ssn = '33' THEN NULL
        ELSE prov_facility_ssn
    END as prov_facility_ssn,
    CASE
        WHEN prov_facility_state_license = '5' THEN NULL
        WHEN prov_facility_state_license = '05' THEN NULL
        WHEN prov_facility_state_license = '6' THEN NULL
        WHEN prov_facility_state_license = '06' THEN NULL
        WHEN prov_facility_state_license = '7' THEN NULL
        WHEN prov_facility_state_license = '07' THEN NULL
        WHEN prov_facility_state_license = '8' THEN NULL
        WHEN prov_facility_state_license = '08' THEN NULL
        WHEN prov_facility_state_license = '9' THEN NULL
        WHEN prov_facility_state_license = '09' THEN NULL
        WHEN prov_facility_state_license = '12' THEN NULL
        WHEN prov_facility_state_license = '13' THEN NULL
        WHEN prov_facility_state_license = '14' THEN NULL
        WHEN prov_facility_state_license = '33' THEN NULL
        ELSE prov_facility_state_license
    END as prov_facility_state_license,
    CASE
        WHEN prov_facility_upin = '5' THEN NULL
        WHEN prov_facility_upin = '05' THEN NULL
        WHEN prov_facility_upin = '6' THEN NULL
        WHEN prov_facility_upin = '06' THEN NULL
        WHEN prov_facility_upin = '7' THEN NULL
        WHEN prov_facility_upin = '07' THEN NULL
        WHEN prov_facility_upin = '8' THEN NULL
        WHEN prov_facility_upin = '08' THEN NULL
        WHEN prov_facility_upin = '9' THEN NULL
        WHEN prov_facility_upin = '09' THEN NULL
        WHEN prov_facility_upin = '12' THEN NULL
        WHEN prov_facility_upin = '13' THEN NULL
        WHEN prov_facility_upin = '14' THEN NULL
        WHEN prov_facility_upin = '33' THEN NULL
        ELSE prov_facility_upin
    END as prov_facility_upin,
    CASE
        WHEN prov_facility_commercial_id = '5' THEN NULL
        WHEN prov_facility_commercial_id = '05' THEN NULL
        WHEN prov_facility_commercial_id = '6' THEN NULL
        WHEN prov_facility_commercial_id = '06' THEN NULL
        WHEN prov_facility_commercial_id = '7' THEN NULL
        WHEN prov_facility_commercial_id = '07' THEN NULL
        WHEN prov_facility_commercial_id = '8' THEN NULL
        WHEN prov_facility_commercial_id = '08' THEN NULL
        WHEN prov_facility_commercial_id = '9' THEN NULL
        WHEN prov_facility_commercial_id = '09' THEN NULL
        WHEN prov_facility_commercial_id = '12' THEN NULL
        WHEN prov_facility_commercial_id = '13' THEN NULL
        WHEN prov_facility_commercial_id = '14' THEN NULL
        WHEN prov_facility_commercial_id = '33' THEN NULL
        ELSE prov_facility_commercial_id
    END as prov_facility_commercial_id,
    CASE
        WHEN prov_facility_name_1 = '5' THEN NULL
        WHEN prov_facility_name_1 = '05' THEN NULL
        WHEN prov_facility_name_1 = '6' THEN NULL
        WHEN prov_facility_name_1 = '06' THEN NULL
        WHEN prov_facility_name_1 = '7' THEN NULL
        WHEN prov_facility_name_1 = '07' THEN NULL
        WHEN prov_facility_name_1 = '8' THEN NULL
        WHEN prov_facility_name_1 = '08' THEN NULL
        WHEN prov_facility_name_1 = '9' THEN NULL
        WHEN prov_facility_name_1 = '09' THEN NULL
        WHEN prov_facility_name_1 = '12' THEN NULL
        WHEN prov_facility_name_1 = '13' THEN NULL
        WHEN prov_facility_name_1 = '14' THEN NULL
        WHEN prov_facility_name_1 = '33' THEN NULL
        ELSE prov_facility_name_1
    END as prov_facility_name_1,
    CASE
        WHEN prov_facility_name_2 = '5' THEN NULL
        WHEN prov_facility_name_2 = '05' THEN NULL
        WHEN prov_facility_name_2 = '6' THEN NULL
        WHEN prov_facility_name_2 = '06' THEN NULL
        WHEN prov_facility_name_2 = '7' THEN NULL
        WHEN prov_facility_name_2 = '07' THEN NULL
        WHEN prov_facility_name_2 = '8' THEN NULL
        WHEN prov_facility_name_2 = '08' THEN NULL
        WHEN prov_facility_name_2 = '9' THEN NULL
        WHEN prov_facility_name_2 = '09' THEN NULL
        WHEN prov_facility_name_2 = '12' THEN NULL
        WHEN prov_facility_name_2 = '13' THEN NULL
        WHEN prov_facility_name_2 = '14' THEN NULL
        WHEN prov_facility_name_2 = '33' THEN NULL
        ELSE prov_facility_name_2
    END as prov_facility_name_2,
    CASE
        WHEN prov_facility_address_1 = '5' THEN NULL
        WHEN prov_facility_address_1 = '05' THEN NULL
        WHEN prov_facility_address_1 = '6' THEN NULL
        WHEN prov_facility_address_1 = '06' THEN NULL
        WHEN prov_facility_address_1 = '7' THEN NULL
        WHEN prov_facility_address_1 = '07' THEN NULL
        WHEN prov_facility_address_1 = '8' THEN NULL
        WHEN prov_facility_address_1 = '08' THEN NULL
        WHEN prov_facility_address_1 = '9' THEN NULL
        WHEN prov_facility_address_1 = '09' THEN NULL
        WHEN prov_facility_address_1 = '12' THEN NULL
        WHEN prov_facility_address_1 = '13' THEN NULL
        WHEN prov_facility_address_1 = '14' THEN NULL
        WHEN prov_facility_address_1 = '33' THEN NULL
        ELSE prov_facility_address_1
    END as prov_facility_address_1,
    CASE
        WHEN prov_facility_address_2 = '5' THEN NULL
        WHEN prov_facility_address_2 = '05' THEN NULL
        WHEN prov_facility_address_2 = '6' THEN NULL
        WHEN prov_facility_address_2 = '06' THEN NULL
        WHEN prov_facility_address_2 = '7' THEN NULL
        WHEN prov_facility_address_2 = '07' THEN NULL
        WHEN prov_facility_address_2 = '8' THEN NULL
        WHEN prov_facility_address_2 = '08' THEN NULL
        WHEN prov_facility_address_2 = '9' THEN NULL
        WHEN prov_facility_address_2 = '09' THEN NULL
        WHEN prov_facility_address_2 = '12' THEN NULL
        WHEN prov_facility_address_2 = '13' THEN NULL
        WHEN prov_facility_address_2 = '14' THEN NULL
        WHEN prov_facility_address_2 = '33' THEN NULL
        ELSE prov_facility_address_2
    END as prov_facility_address_2,
    CASE
        WHEN prov_facility_city = '5' THEN NULL
        WHEN prov_facility_city = '05' THEN NULL
        WHEN prov_facility_city = '6' THEN NULL
        WHEN prov_facility_city = '06' THEN NULL
        WHEN prov_facility_city = '7' THEN NULL
        WHEN prov_facility_city = '07' THEN NULL
        WHEN prov_facility_city = '8' THEN NULL
        WHEN prov_facility_city = '08' THEN NULL
        WHEN prov_facility_city = '9' THEN NULL
        WHEN prov_facility_city = '09' THEN NULL
        WHEN prov_facility_city = '12' THEN NULL
        WHEN prov_facility_city = '13' THEN NULL
        WHEN prov_facility_city = '14' THEN NULL
        WHEN prov_facility_city = '33' THEN NULL
        ELSE prov_facility_city
    END as prov_facility_city,
    CASE
        WHEN prov_facility_state = '5' THEN NULL
        WHEN prov_facility_state = '05' THEN NULL
        WHEN prov_facility_state = '6' THEN NULL
        WHEN prov_facility_state = '06' THEN NULL
        WHEN prov_facility_state = '7' THEN NULL
        WHEN prov_facility_state = '07' THEN NULL
        WHEN prov_facility_state = '8' THEN NULL
        WHEN prov_facility_state = '08' THEN NULL
        WHEN prov_facility_state = '9' THEN NULL
        WHEN prov_facility_state = '09' THEN NULL
        WHEN prov_facility_state = '12' THEN NULL
        WHEN prov_facility_state = '13' THEN NULL
        WHEN prov_facility_state = '14' THEN NULL
        WHEN prov_facility_state = '33' THEN NULL
        ELSE prov_facility_state
    END as prov_facility_state,
    CASE
        WHEN prov_facility_zip = '5' THEN NULL
        WHEN prov_facility_zip = '05' THEN NULL
        WHEN prov_facility_zip = '6' THEN NULL
        WHEN prov_facility_zip = '06' THEN NULL
        WHEN prov_facility_zip = '7' THEN NULL
        WHEN prov_facility_zip = '07' THEN NULL
        WHEN prov_facility_zip = '8' THEN NULL
        WHEN prov_facility_zip = '08' THEN NULL
        WHEN prov_facility_zip = '9' THEN NULL
        WHEN prov_facility_zip = '09' THEN NULL
        WHEN prov_facility_zip = '12' THEN NULL
        WHEN prov_facility_zip = '13' THEN NULL
        WHEN prov_facility_zip = '14' THEN NULL
        WHEN prov_facility_zip = '33' THEN NULL
        ELSE prov_facility_zip
    END as prov_facility_zip,
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
    part_provider,
    CASE
    WHEN part_processdate IN ('NULL', '0_PREDATES_HVM_HISTORY') THEN '0_PREDATES_HVM_HISTORY'
    WHEN part_provider IN ('ability', 'allscripts')
    THEN CONCAT(REGEXP_REPLACE(part_processdate, '-', '/'), '/01')
    ELSE part_processdate
    END AS part_processdate
FROM default.medicalclaims_old
WHERE part_provider IN ('ability', 'navicure', 'allscripts')
;
