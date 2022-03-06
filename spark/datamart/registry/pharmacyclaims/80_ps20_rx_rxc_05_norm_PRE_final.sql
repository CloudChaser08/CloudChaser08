SELECT
    txn.claim_id,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.patient_year_of_birth,
    txn.date_service,
    txn.transaction_code_vendor,
    txn.ndc_code,
    txn.dispensed_quantity,
    txn.days_supply,
    ----- Added 2022-03-01
    txn.prov_prescribing_std_taxonomy,
    ---- Will be added after approval
    --txn.submitted_gross_due,
    txn.prov_dispensing_id,
    txn.prov_dispensing_qual,
	txn.prov_prescribing_id,
	txn.prov_prescribing_qual,
    txn.logical_delete_reason,
    txn.stg_file_date,
    txn.part_provider,
    txn.part_best_date
FROM ps20_rx_rxc_03_norm_comb_hist_cf txn
WHERE
--------------- Exclude REVERSE status rows from current feed
NOT EXISTS
 (
     SELECT 1
     FROM ps20_rx_rxc_04_norm_reverse_status reverse
     WHERE txn.claim_id = reverse.claim_id
 )
--------------- Exclude REVERSED status rows
AND NOT EXISTS
 (
     SELECT 1
     FROM ps20_rx_rxc_04_norm_reversed reversed
     WHERE txn.claim_id = reversed.claim_id
 )


UNION ALL
--------------------- INCLUDE REVERSE status
SELECT
    txn.claim_id,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.patient_year_of_birth,
    txn.date_service,
    txn.transaction_code_vendor,
    txn.ndc_code,
    txn.dispensed_quantity,
    txn.days_supply,
    ----- Added 2022-03-01
    txn.prov_prescribing_std_taxonomy,
    ---- Will be added after approval
    --txn.submitted_gross_due,
    txn.prov_dispensing_id,
    txn.prov_dispensing_qual,
	txn.prov_prescribing_id,
	txn.prov_prescribing_qual,
    txn.logical_delete_reason,
    txn.stg_file_date,
    txn.part_provider,
    txn.part_best_date
FROM ps20_rx_rxc_04_norm_reverse_status txn

UNION ALL
--------------------- INCLUDE REVERSED

SELECT
    txn.claim_id,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.patient_year_of_birth,
    txn.date_service,
    txn.transaction_code_vendor,
    txn.ndc_code,
    txn.dispensed_quantity,
    txn.days_supply,
    ----- Added 2022-03-01
    txn.prov_prescribing_std_taxonomy,
    ---- Will be added after approval
    --txn.submitted_gross_due,
    txn.prov_dispensing_id,
    txn.prov_dispensing_qual,
	txn.prov_prescribing_id,
	txn.prov_prescribing_qual,
    txn.logical_delete_reason,
    txn.stg_file_date,
    txn.part_provider,
    txn.part_best_date
FROM ps20_rx_rxc_04_norm_reversed txn



