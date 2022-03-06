SELECT
 MONOTONICALLY_INCREASING_ID()  AS record_id,
 *
 FROM
(
SELECT
    'pregnancy'                AS registry_type,
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
    logical_delete_reason,
    txn.stg_file_date,
    txn.part_provider,
    txn.part_best_date
FROM ps20_rx_rxc_05_norm_PRE_final txn
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10,
         11,12,13,14,15,16,17,18,19,20,21,22
         ,23,24
)
