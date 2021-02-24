SELECT
txn.*
FROM inovalon_03_norm_comb_hist_cf txn
WHERE 
--------------- Exclude REVERSE status rows from current feed
NOT EXISTS
(
    SELECT 1
    FROM inovalon_04_norm_reverse_status reverse
    WHERE txn.claim_id = reverse.claim_id
)
--------------- Exclude REVERSED status rows 
AND NOT EXISTS
(
    SELECT 1
    FROM inovalon_04_norm_reversed reversed
    WHERE txn.claim_id = reversed.claim_id
)


-----------------------------------------------------------------------------------------
------------------------------------ UNION REVERSE transaction
----------------------------------------------------------------------------------------- 
UNION ALL
SELECT
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
    pharmacy_npi,
    prov_prescribing_npi,
    prov_prescribing_name_1,
    prov_prescribing_address_1,
    prov_prescribing_address_2,
    prov_prescribing_city,
    prov_prescribing_state,
    prov_prescribing_zip,
    prov_prescribing_std_taxonomy,
    prov_prescribing_vendor_specialty,
--  copay_coinsurance,      
    submitted_gross_due,
    paid_gross_due,
	prov_prescribing_id,
	prov_prescribing_qual,  
    logical_delete_reason,
    part_provider,
    part_best_date
FROM inovalon_04_norm_reverse_status txn
-----------------------------------------------------------------------------------------
------------------------------------ UNION ALL REVERSED Transaction
----------------------------------------------------------------------------------------- 
UNION ALL
SELECT
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
    pharmacy_npi,
    prov_prescribing_npi,
    prov_prescribing_name_1,
    prov_prescribing_address_1,
    prov_prescribing_address_2,
    prov_prescribing_city,
    prov_prescribing_state,
    prov_prescribing_zip,
    prov_prescribing_std_taxonomy,
    prov_prescribing_vendor_specialty,
--  copay_coinsurance,      
    submitted_gross_due,
    paid_gross_due,
	prov_prescribing_id,
	prov_prescribing_qual,  
    logical_delete_reason,
    part_provider,
    part_best_date
FROM inovalon_04_norm_reversed



