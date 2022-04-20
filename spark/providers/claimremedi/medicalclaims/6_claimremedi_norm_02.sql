SELECT 
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    patient_gender,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    claim_type,
    date_received,
    ------------------------------------------------------------------------------------------------
	--  if date_service is NULL, set it to date_service_end. If NULL just swap with date_service_end		
    ------------------------------------------------------------------------------------------------
	CASE					
	    WHEN COALESCE(claim_type, 'X') = 'P'					
	        THEN COALESCE(date_service, date_service_end)					
        ELSE date_service						
	END                                            AS date_service,
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
    inst_admit_source_std_id,
    inst_discharge_status_std_id,
    inst_type_of_bill_std_id,
    inst_drg_std_id,
    place_of_service_std_id,
    service_line_number,
    service_line_id,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_priority,
    admit_diagnosis_ind,
    procedure_code,
    procedure_code_qual,
    principal_proc_ind,
    procedure_units_billed,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    revenue_code,
    ndc_code,
    medical_coverage_type,
    line_charge,
    total_charge,
    prov_rendering_npi,
    prov_referring_npi,
    prov_facility_npi,
    payer_name,
    payer_plan_id,
    prov_rendering_state_license,
    prov_rendering_upin,
    prov_rendering_commercial_id,
    prov_rendering_name_1,
    prov_rendering_name_2,
    prov_rendering_std_taxonomy,
    prov_billing_tax_id,
    prov_billing_state_license,
    prov_billing_upin,
    prov_billing_name_1,
    prov_billing_name_2,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_referring_state_license,
    prov_referring_upin,
    prov_referring_commercial_id,
    prov_referring_name_1,
    prov_referring_name_2,
    prov_facility_state_license,
    prov_facility_commercial_id,
    prov_facility_name_1,
    prov_facility_address_1,
    prov_facility_address_2,
    prov_facility_city,
    prov_facility_state,
    prov_facility_zip,
    part_provider,
--    CONCAT(SUBSTR(COALESCE(date_service, date_service_end), 1, 7), '-01')           AS part_best_date
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(CONCAT(SUBSTR(COALESCE(date_service, date_service_end), 1, 7), '-01'), '%Y-%m-%d') AS DATE),
                                            CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ),
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    CONCAT(SUBSTR(COALESCE(date_service, date_service_end), 1, 7), '-01')
                )
	END                                                                                     AS part_best_date



FROM claimremedi_norm_01
WHERE COALESCE(claim_type, 'X') <> 'P'
    OR date_service IS NULL
    OR date_service_end IS NULL
    OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) = 0
    OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) > 365
