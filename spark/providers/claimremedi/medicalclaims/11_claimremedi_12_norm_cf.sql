SELECT MONOTONICALLY_INCREASING_ID() AS record_id, * FROM
(
    ------------------------------------------------------------------------------------------------
	--  data that doesn't need to have the dates exploded  UNION data that need to have the dates exploded
    ------------------------------------------------------------------------------------------------
    SELECT
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
        --date_service,
        ------------------------------------------------------------------------------------------------
        --  if date_service is NULL, set it to date_service_end. If NULL just swap with date_service_end
        ------------------------------------------------------------------------------------------------
        CASE
            WHEN COALESCE(claim_type, 'X') = 'P'
                THEN COALESCE(date_service, date_service_end)
            ELSE date_service
        END                                            AS date_service,
        --date_service_end,
        ------------------------------------------------------------------------------------------------
        --  if date_service_end is NULL, set it to date_service. If NULL just swap with date_service
        ------------------------------------------------------------------------------------------------
            CASE
            WHEN COALESCE(claim_type, 'X') = 'P'
                THEN COALESCE(date_service_end, date_service)
            ELSE date_service_end
        END                                            AS date_service_end,
       ----------------------------------------------------------------------------------
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
        CASE
            WHEN COALESCE(date_service, date_service_end) IS NULL
                THEN '0_PREDATES_HVM_HISTORY'
            ELSE
                SUBSTR(COALESCE(date_service, date_service_end), 1, 7)
            END AS part_best_date
    FROM
        claimremedi_11_norm_cf_prep
    WHERE
        COALESCE(claim_type, 'X') <> 'P'
        OR date_service IS NULL
        OR date_service_end IS NULL
        OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) = 0
        OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) > 365
UNION ALL
    ------------------------------------------------------------------------------------------------
	--  data that doesn't need to have the dates exploded  UNION data that need to have the dates exploded
    ------------------------------------------------------------------------------------------------
    SELECT
        clm.claim_id,
        clm.hvid,
        clm.created,
        clm.model_version,
        clm.data_set,
        clm.data_feed,
        clm.data_vendor,
        clm.source_version,
        clm.patient_gender,
        clm.patient_age,
        clm.patient_year_of_birth,
        clm.patient_zip3,
        clm.patient_state,
        clm.claim_type,
        clm.date_received,
        --clm.date_service,
        DATE_ADD (clm.date_service,dtexplode.i)   as date_service,
        --clm.date_service_end,
        DATE_ADD (clm.date_service,dtexplode.i)   as date_service_end,
        clm.inst_date_admitted,
        clm.inst_date_discharged,
        clm.inst_admit_type_std_id,
        clm.inst_admit_type_vendor_id,
        clm.inst_admit_type_vendor_desc,
        clm.inst_admit_source_std_id,
        clm.inst_admit_source_vendor_id,
        clm.inst_admit_source_vendor_desc,
        clm.inst_discharge_status_std_id,
        clm.inst_discharge_status_vendor_id,
        clm.inst_discharge_status_vendor_desc,
        clm.inst_type_of_bill_std_id,
        clm.inst_type_of_bill_vendor_id,
        clm.inst_type_of_bill_vendor_desc,
        clm.inst_drg_std_id,
        clm.inst_drg_vendor_id,
        clm.inst_drg_vendor_desc,
        clm.place_of_service_std_id,
        clm.place_of_service_vendor_id,
        clm.place_of_service_vendor_desc,
        clm.service_line_number,
        clm.diagnosis_code,
        clm.diagnosis_code_qual,
        clm.diagnosis_priority,
        clm.admit_diagnosis_ind,
        clm.procedure_code,
        clm.procedure_code_qual,
        clm.principal_proc_ind,
        clm.procedure_units,
        clm.procedure_modifier_1,
        clm.procedure_modifier_2,
        clm.procedure_modifier_3,
        clm.procedure_modifier_4,
        clm.revenue_code,
        clm.ndc_code,
        clm.medical_coverage_type,
        clm.line_charge,
        clm.line_allowed,
        clm.total_charge,
        clm.total_allowed,
        clm.prov_rendering_npi,
        clm.prov_billing_npi,
        clm.prov_referring_npi,
        clm.prov_facility_npi,
        clm.payer_vendor_id,
        clm.payer_name,
        clm.payer_parent_name,
        clm.payer_org_name,
        clm.payer_plan_id,
        clm.payer_plan_name,
        clm.payer_type,
        clm.prov_rendering_vendor_id,
        clm.prov_rendering_tax_id,
        clm.prov_rendering_dea_id,
        clm.prov_rendering_ssn,
        clm.prov_rendering_state_license,
        clm.prov_rendering_upin,
        clm.prov_rendering_commercial_id,
        clm.prov_rendering_name_1,
        clm.prov_rendering_name_2,
        clm.prov_rendering_address_1,
        clm.prov_rendering_address_2,
        clm.prov_rendering_city,
        clm.prov_rendering_state,
        clm.prov_rendering_zip,
        clm.prov_rendering_std_taxonomy,
        clm.prov_rendering_vendor_specialty,
        clm.prov_billing_vendor_id,
        clm.prov_billing_tax_id,
        clm.prov_billing_dea_id,
        clm.prov_billing_ssn,
        clm.prov_billing_state_license,
        clm.prov_billing_upin,
        clm.prov_billing_commercial_id,
        clm.prov_billing_name_1,
        clm.prov_billing_name_2,
        clm.prov_billing_address_1,
        clm.prov_billing_address_2,
        clm.prov_billing_city,
        clm.prov_billing_state,
        clm.prov_billing_zip,
        clm.prov_billing_std_taxonomy,
        clm.prov_billing_vendor_specialty,
        clm.prov_referring_vendor_id,
        clm.prov_referring_tax_id,
        clm.prov_referring_dea_id,
        clm.prov_referring_ssn,
        clm.prov_referring_state_license,
        clm.prov_referring_upin,
        clm.prov_referring_commercial_id,
        clm.prov_referring_name_1,
        clm.prov_referring_name_2,
        clm.prov_referring_address_1,
        clm.prov_referring_address_2,
        clm.prov_referring_city,
        clm.prov_referring_state,
        clm.prov_referring_zip,
        clm.prov_referring_std_taxonomy,
        clm.prov_referring_vendor_specialty,
        clm.prov_facility_vendor_id,
        clm.prov_facility_tax_id,
        clm.prov_facility_dea_id,
        clm.prov_facility_ssn,
        clm.prov_facility_state_license,
        clm.prov_facility_upin,
        clm.prov_facility_commercial_id,
        clm.prov_facility_name_1,
        clm.prov_facility_name_2,
        clm.prov_facility_address_1,
        clm.prov_facility_address_2,
        clm.prov_facility_city,
        clm.prov_facility_state,
        clm.prov_facility_zip,
        clm.prov_facility_std_taxonomy,
        clm.prov_facility_vendor_specialty,
        clm.cob_payer_vendor_id_1,
        clm.cob_payer_seq_code_1,
        clm.cob_payer_hpid_1,
        clm.cob_payer_claim_filing_ind_code_1,
        clm.cob_ins_type_code_1,
        clm.cob_payer_vendor_id_2,
        clm.cob_payer_seq_code_2,
        clm.cob_payer_hpid_2,
        clm.cob_payer_claim_filing_ind_code_2,
        clm.cob_ins_type_code_2,
        clm.part_provider,
        CASE
            WHEN DATE_ADD (clm.date_service,dtexplode.i)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
               OR DATE_ADD (clm.date_service,dtexplode.i)  > CAST('{VDR_FILE_DT}' AS DATE)
                THEN '0_PREDATES_HVM_HISTORY'
            ELSE
                SUBSTR(CAST(DATE_ADD (clm.date_service,dtexplode.i)  AS STRING), 1, 7)
        END                                                                         AS part_best_date
    FROM
        claimremedi_11_norm_cf_prep clm
        lateral view
        posexplode(split(space(datediff(clm.date_service_end,clm.date_service)),' ')) dtexplode as i,x
            WHERE
             COALESCE(clm.claim_type, 'X') = 'P'
            AND clm.date_service IS NOT NULL
            AND clm.date_service_end IS NOT NULL
            AND DATEDIFF(COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)), COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))) <> 0
            AND DATEDIFF(COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)), COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))) <= 365
            AND DATE_ADD (clm.date_service,dtexplode.i) <= COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE))
)


