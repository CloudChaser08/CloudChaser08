SELECT
    record_id,
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
    date_service,
    date_written,
    transaction_code_vendor,
    response_code_vendor,
    reject_reason_code_1,
    reject_reason_code_2,
    reject_reason_code_3,
    reject_reason_code_4,
    reject_reason_code_5,
    diagnosis_code,
    diagnosis_code_qual,
    procedure_code,
    procedure_code_qual,
    ndc_code,
    product_service_id,
    product_service_id_qual,
    rx_number,
    rx_number_qual,
    processor_control_number,
    fill_number,
    refill_auth_amount,
    dispensed_quantity,
    unit_of_measure,
    days_supply,
    pharmacy_npi,
    compound_code,
    unit_dose_indicator,
    dispensed_as_written,
    level_of_service,
    prov_prescribing_npi,
    pharmacy_other_id,
    pharmacy_other_qual,
    prov_prescribing_id,
    prov_prescribing_qual,
    CASE
        WHEN reversed.pharmacy_claim_ref_id IS NOT NULL
            THEN CAST('REVERSED' AS STRING)
        WHEN reversal.pharmacy_claim_id IS NOT NULL
            THEN CAST('REVERSAL' AS STRING)
    ELSE NULL END AS logical_delete_reason,
	part_provider,
    part_best_date
FROM
    esi_04_comb_hist_cf txn
    LEFT OUTER JOIN esi_02_raw_hist reversed
        ON txn.claim_id = reversed.pharmacy_claim_ref_id
    LEFT OUTER JOIN esi_02_raw_hist reversal
        ON txn.claim_id = reversal.pharmacy_claim_id

