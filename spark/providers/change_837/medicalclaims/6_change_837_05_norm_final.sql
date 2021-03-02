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
    inst_admit_type_std_id,
    inst_admit_source_std_id,
    inst_discharge_status_std_id,
    inst_type_of_bill_std_id,
    inst_drg_std_id,
    place_of_service_std_id,
    service_line_number,
    CLEAN_UP_DIAGNOSIS_CODE(diagnosis_code, diagnosis_code_qual, date_service) AS diagnosis_code,
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
    prov_billing_npi,
    prov_referring_npi,
    prov_facility_npi,
    payer_name,
    payer_plan_id,
    prov_rendering_state_license,
    prov_rendering_name_1,
    prov_rendering_std_taxonomy,
    prov_billing_vendor_id,
    prov_billing_ssn,
    prov_billing_state_license,
    prov_billing_name_1,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_referring_name_1,
    prov_facility_state_license,
    prov_facility_name_1,
    prov_facility_address_1,
    prov_facility_address_2,
    prov_facility_city,
    prov_facility_state,
    prov_facility_zip,
    medical_claim_link_text,
    part_provider,
    CONCAT(SUBSTR(COALESCE(date_service, date_service_end), 1, 7), '-01')           AS part_best_date

 FROM  change_837_04_norm_prefinal  
             
WHERE COALESCE(claim_type, 'X') <> 'P'
   OR date_service IS NULL
   OR date_service_end IS NULL
   OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) = 0
   OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) > 365

UNION ALL
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
    patient_gender,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    claim_type,
    date_received,
     --service_from,
    DATE_ADD (clm.date_service,dtexplode.i)   as date_service,
    --service_to,
    DATE_ADD (clm.date_service,dtexplode.i)   as date_service_end,
    inst_date_admitted,
    inst_admit_type_std_id,
    inst_admit_source_std_id,
    inst_discharge_status_std_id,
    inst_type_of_bill_std_id,
    inst_drg_std_id,
    place_of_service_std_id,
    service_line_number,
    CLEAN_UP_DIAGNOSIS_CODE(diagnosis_code, diagnosis_code_qual, date_service) AS diagnosis_code,
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
    prov_billing_npi,
    prov_referring_npi,
    prov_facility_npi,
    payer_name,
    payer_plan_id,
    prov_rendering_state_license,
    prov_rendering_name_1,
    prov_rendering_std_taxonomy,
    prov_billing_vendor_id,
    prov_billing_ssn,
    prov_billing_state_license,
    prov_billing_name_1,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_referring_name_1,
    prov_facility_state_license,
    prov_facility_name_1,
    prov_facility_address_1,
    prov_facility_address_2,
    prov_facility_city,
    prov_facility_state,
    prov_facility_zip,
    medical_claim_link_text,
    part_provider,
    CASE 
        WHEN DATE_ADD (clm.date_service,dtexplode.i)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
           OR DATE_ADD (clm.date_service,dtexplode.i)  > CAST('{VDR_FILE_DT}' AS DATE)
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                 SUBSTR(CAST(DATE_ADD (clm.date_service,dtexplode.i)  AS STRING), 1, 7), '-01'
                )
    END                                                                         AS part_best_date

FROM  change_837_04_norm_prefinal clm
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
       

