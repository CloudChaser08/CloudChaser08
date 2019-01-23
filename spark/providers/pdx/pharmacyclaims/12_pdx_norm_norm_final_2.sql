SELECT
    record_id,
	claim_id,
	hvid,
    created,
	model_version,
	/* This represents the original dataset name from the initial load. */
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
 FROM pdx_norm_hist_wo_revd
