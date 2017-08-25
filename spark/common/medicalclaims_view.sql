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
    CASE
    WHEN part_processdate IN ('NULL', '0_PREDATES_HVM_HISTORY') THEN '0_PREDATES_HVM_HISTORY'
    WHEN part_provider IN ('ability', 'allscripts')
    THEN CONCAT(REGEXP_REPLACE(part_processdate, '-', '/'), '/01')
    ELSE part_processdate
    END AS part_processdate
FROM default.medicalclaims_old
WHERE part_provider IN ('ability', 'navicure', 'allscripts')
;
