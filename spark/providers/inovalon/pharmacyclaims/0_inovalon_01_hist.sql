SELECT 
   -- Notice Record ID is not brought from history
    claim_id,
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
    date_service,
    transaction_code_vendor,
    ndc_code,
    dispensed_quantity,
    days_supply,
    prov_dispensing_npi,
    prov_prescribing_npi,
    prov_prescribing_name_1,
    prov_prescribing_address_1,
    prov_prescribing_address_2,
    prov_prescribing_city,
    prov_prescribing_state,
    prov_prescribing_zip,
    prov_prescribing_std_taxonomy,
    prov_prescribing_vendor_specialty,
    copay_coinsurance,      
    submitted_gross_due,
    paid_gross_due,
	prov_prescribing_id,
	prov_prescribing_qual,  
    logical_delete_reason,
    part_provider,
    part_best_date
 
FROM previous_run_from_transformed
    WHERE part_provider = 'inovalon'
    ----- Look current month and 3 month back
    AND part_best_date >= ADD_MONTHS(TRUNC(CAST('{VDR_FILE_DT}' AS DATE), 'MONTH'), -2) 
    AND part_best_date <= ADD_MONTHS(TRUNC(CAST('{VDR_FILE_DT}' AS DATE), 'MONTH'),  0)
