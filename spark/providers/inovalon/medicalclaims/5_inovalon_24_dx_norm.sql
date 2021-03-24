SELECT
    hv_enc_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    patient_gender,
    patient_age,
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
    inst_discharge_status_std_id,
    inst_type_of_bill_std_id,
    inst_drg_std_id,
    inst_drg_vendor_id,
    inst_drg_vendor_desc,
    place_of_service_std_id,
    service_line_id,
    diagnosis_code,
    diagnosis_code_qual,
    admit_diagnosis_ind,
    procedure_code,
    procedure_code_qual,
    procedure_units_billed,
    procedure_modifier_1,   
    revenue_code,
    total_charge,  --- new field
    prov_rendering_npi,
    prov_billing_npi,
    prov_rendering_vendor_id,
    prov_rendering_name_1,
    prov_rendering_address_1,
    prov_rendering_address_2,
    prov_rendering_city,
    prov_rendering_state,
    prov_rendering_zip,
    prov_billing_vendor_id,
    prov_billing_name_1,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    logical_delete_reason,
    part_provider,
    part_best_date
 FROM  inovalon_23_dx_norm_cf

WHERE COALESCE(claim_type, 'X') <> 'P'
   OR date_service IS NULL
   OR date_service_end IS NULL
   OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) = 0
   OR DATEDIFF (COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),COALESCE(date_service, CAST('1900-01-01' AS DATE))) > 365

UNION ALL
------------------------------------------------------------------------------------------------
--  Data that needs to have the dates exploded
------------------------------------------------------------------------------------------------
SELECT
    hv_enc_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    patient_gender,
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    claim_type,
    date_received,
        --service_from,
        DATE_ADD (clm.date_service,dtexplode.i)   as date_service,
        --service_to,
        DATE_ADD (clm.date_service,dtexplode.i)   as date_service_end,

    inst_discharge_status_std_id,
    inst_type_of_bill_std_id,
    inst_drg_std_id,
    inst_drg_vendor_id,
    inst_drg_vendor_desc,
    place_of_service_std_id,
    service_line_id,
    diagnosis_code,
    diagnosis_code_qual,
    admit_diagnosis_ind,
    procedure_code,
    procedure_code_qual,
    procedure_units_billed,
    procedure_modifier_1,
    revenue_code,
    total_charge, --- New fields 2021-03-16
    prov_rendering_npi,
    prov_billing_npi,
    prov_rendering_vendor_id,
    prov_rendering_name_1,
    prov_rendering_address_1,
    prov_rendering_address_2,
    prov_rendering_city,
    prov_rendering_state,
    prov_rendering_zip,
    prov_billing_vendor_id,
    prov_billing_name_1,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    logical_delete_reason,
    part_provider,
    CASE
        WHEN CAP_DATE
                (
                    DATE_ADD (clm.date_service,dtexplode.i),
                    CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
             THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                    SUBSTR(CAST(DATE_ADD (clm.date_service,dtexplode.i)  AS STRING), 1, 7), '-01'
                )
    END 								AS part_best_date


FROM  inovalon_23_dx_norm_cf clm
    lateral view
        posexplode(split(space(datediff(clm.date_service_end,clm.date_service)),' ')) dtexplode as i,x
WHERE
    COALESCE(clm.claim_type, 'X') = 'P'
    AND clm.date_service IS NOT NULL
    AND clm.date_service_end IS NOT NULL
    AND DATEDIFF(COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)), COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))) <> 0
    AND DATEDIFF(COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)), COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))) <= 365
    AND DATE_ADD (clm.date_service,dtexplode.i) <= COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE))
