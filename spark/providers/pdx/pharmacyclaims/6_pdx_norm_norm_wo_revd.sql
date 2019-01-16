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
	patient_age,
	patient_year_of_birth,
	patient_zip3,
	patient_state,
	date_service,
	date_written,
	response_code_vendor,
	diagnosis_code,
	diagnosis_code_qual,
	ndc_code,
	rx_number,
	rx_number_qual,
	bin_number,
	processor_control_number,
	fill_number,
	refill_auth_amount,
	dispensed_quantity,
	unit_of_measure,
	days_supply,
	pharmacy_npi,
	payer_name,
	payer_type,
	compound_code,
	dispensed_as_written,
	prescription_origin,
	orig_prescribed_product_service_code,
	orig_prescribed_product_service_code_qual,
	level_of_service,
	prov_prescribing_npi,
	prov_prescribing_dea_id,
	prov_prescribing_state_license,
	prov_prescribing_name_1,
	prov_prescribing_name_2,
	prov_prescribing_city,
	prov_prescribing_state,
	prov_prescribing_zip,
	cob_count,
	copay_coinsurance,
	basis_of_cost_determination,
	submitted_ingredient_cost,
	submitted_dispensing_fee,
	basis_of_reimbursement_determination,
	paid_ingredient_cost,
	paid_dispensing_fee,
	paid_gross_due,
	paid_patient_pay,
	coupon_type,
	coupon_number,
	coupon_value,
	pharmacy_other_id,
	pharmacy_other_qual,
	pharmacy_postal_code,
	logical_delete_reason_date,
	logical_delete_reason,
	part_provider,
	part_best_date
 FROM pdx_norm_txn_norm nrm
WHERE nrm.logical_delete_reason IS NOT NULL
   OR UPPER(COALESCE(nrm.response_code_vendor, '')) <> 'D'
   OR UPPER(COALESCE(nrm.payer_type, '')) = 'CASH'
  /* Check to see if there's already a Reversed Claim that matches this row */
  /* (based on the reversal matching logic) in the newly normalized data. */
  /* With the current normalization logic, this should never happen, but */
  /* the logic is here, just in case the normalization logic changes. */
   OR EXISTS
    (
        SELECT 1
         FROM pdx_norm_txn_norm rv1
        WHERE COALESCE(rv1.logical_delete_reason, '') = 'Reversed Claim'
          AND COALESCE(nrm.rx_number, '') = COALESCE(rv1.rx_number, '')
          AND COALESCE(nrm.pharmacy_other_id, '') = COALESCE(rv1.pharmacy_other_id, '')
          /* Don't match if either date is NULL. */
          AND COALESCE(nrm.logical_delete_reason_date, CAST('1900-01-01' AS DATE)) =
              COALESCE(rv1.logical_delete_reason_date, CAST('1901-01-01' AS DATE))
          AND COALESCE(nrm.cob_count, '') = COALESCE(rv1.cob_count, '')
          AND CAST(COALESCE(nrm.paid_ingredient_cost, 0) AS FLOAT) = CAST(COALESCE(rv1.paid_ingredient_cost, 0) AS FLOAT)
          AND (CAST(COALESCE(nrm.paid_patient_pay, 0) AS FLOAT) + CAST(COALESCE(nrm.paid_gross_due, 0) AS FLOAT)) =
              (CAST(COALESCE(rv1.paid_patient_pay, 0) AS FLOAT) + CAST(COALESCE(rv1.paid_gross_due, 0) AS FLOAT))
          AND
            (
                COALESCE(rv1.ndc_code, '00000000000') = '00000000000'
             OR COALESCE(nrm.ndc_code, '00000000000') = '00000000000'
             OR COALESCE(nrm.ndc_code, '') = COALESCE(rv1.ndc_code, '')
            )
    )
  /* Check to see if there's already a Reversed Claim that matches this row */
  /* (based on the reversal matching logic) in the historical data. */
  OR EXISTS
    (
        SELECT 1
         FROM pdx_norm_hist rv2
        WHERE COALESCE(rv2.logical_delete_reason, '') = 'Reversed Claim'
          AND COALESCE(nrm.rx_number, '') = COALESCE(rv2.rx_number, '')
          AND COALESCE(nrm.pharmacy_other_id, '') = COALESCE(rv2.pharmacy_other_id, '')
          /* Don't match if either date is NULL. */
          AND COALESCE(nrm.logical_delete_reason_date, CAST('1900-01-01' AS DATE)) =
              COALESCE(rv2.logical_delete_reason_date, CAST('1901-01-01' AS DATE))
          AND COALESCE(nrm.cob_count, '') = COALESCE(rv2.cob_count, '')
          AND CAST(COALESCE(nrm.paid_ingredient_cost, 0) AS FLOAT) = CAST(COALESCE(rv2.paid_ingredient_cost, 0) AS FLOAT)
          AND (CAST(COALESCE(nrm.paid_patient_pay, 0) AS FLOAT) + CAST(COALESCE(nrm.paid_gross_due, 0) AS FLOAT)) =
              (CAST(COALESCE(rv2.paid_patient_pay, 0) AS FLOAT) + CAST(COALESCE(rv2.paid_gross_due, 0) AS FLOAT))
          AND
            (
                COALESCE(rv2.ndc_code, '00000000000') = '00000000000'
             OR COALESCE(nrm.ndc_code, '00000000000') = '00000000000'
             OR COALESCE(nrm.ndc_code, '') = COALESCE(rv2.ndc_code, '')
            )
    )