DROP TABLE IF EXISTS emdeon_rx_trimmed;
CREATE TABLE emdeon_rx_trimmed AS
SELECT
CASE WHEN trim(record_id) = '' THEN NULL ELSE trim(record_id) END AS record_id,
extract_date(date_authorized, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)) AS date_authorized,
CASE WHEN trim(time_authorized) = '' THEN NULL ELSE trim(time_authorized) END AS time_authorized,
CASE WHEN trim(bin_number) = '' THEN NULL ELSE trim(bin_number) END AS bin_number,
CASE WHEN trim(version_number) = '' THEN NULL ELSE trim(version_number) END AS version_number,
CASE WHEN trim(transaction_code) = '' THEN NULL ELSE trim(transaction_code) END AS transaction_code,
CASE WHEN trim(processor_control_number) = '' THEN NULL ELSE trim(processor_control_number) END AS processor_control_number,
CASE WHEN trim(transaction_count) = '' THEN NULL ELSE trim(transaction_count) END AS transaction_count,
CASE WHEN trim(service_provider_id) = '' THEN NULL ELSE trim(service_provider_id) END AS service_provider_id,
CASE WHEN trim(service_provider_id_qualifier) = '' THEN NULL ELSE trim(service_provider_id_qualifier) END AS service_provider_id_qualifier,
extract_date(date_service, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)) AS date_service,
CASE WHEN trim(year_of_birth) = '' THEN NULL ELSE trim(year_of_birth) END AS year_of_birth,
CASE WHEN trim(gender_code) = '' THEN NULL ELSE trim(gender_code) END AS gender_code,
CASE WHEN trim(patient_location) = '' THEN NULL ELSE trim(patient_location) END AS patient_location,
CASE WHEN trim(patient_first_name) = '' THEN NULL ELSE trim(patient_first_name) END AS patient_first_name,
CASE WHEN trim(patient_last_name) = '' THEN NULL ELSE trim(patient_last_name) END AS patient_last_name,
CASE WHEN trim(patient_street_address) = '' THEN NULL ELSE trim(patient_street_address) END AS patient_street_address,
CASE WHEN trim(patient_state_province) = '' THEN NULL ELSE trim(patient_state_province) END AS patient_state_province,
CASE WHEN trim(patient_zip3) = '' THEN NULL ELSE trim(patient_zip3) END AS patient_zip3,
CASE WHEN trim(patient_id_qualifier) = '' THEN NULL ELSE trim(patient_id_qualifier) END AS patient_id_qualifier,
CASE WHEN trim(patient_id) = '' THEN NULL ELSE trim(patient_id) END AS patient_id,
CASE WHEN trim(provider_id) = '' THEN NULL ELSE trim(provider_id) END AS provider_id,
CASE WHEN trim(prov_dispensing_qual) = '' THEN NULL ELSE trim(prov_dispensing_qual) END AS prov_dispensing_qual,
CASE WHEN trim(prescriber_id) = '' THEN NULL ELSE trim(prescriber_id) END AS prescriber_id,
CASE WHEN trim(primary_care_provider_id) = '' THEN NULL ELSE trim(primary_care_provider_id) END AS primary_care_provider_id,
CASE WHEN trim(prescriber_last_name) = '' THEN NULL ELSE trim(prescriber_last_name) END AS prescriber_last_name,
CASE WHEN trim(prov_prescribing_qual) = '' THEN NULL ELSE trim(prov_prescribing_qual) END AS prov_prescribing_qual,
CASE WHEN trim(prov_primary_care_qual) = '' THEN NULL ELSE trim(prov_primary_care_qual) END AS prov_primary_care_qual,
CASE WHEN trim(group_id) = '' THEN NULL ELSE trim(group_id) END AS group_id,
CASE WHEN trim(cardholder_id) = '' THEN NULL ELSE trim(cardholder_id) END AS cardholder_id,
CASE WHEN trim(person_code) = '' THEN NULL ELSE trim(person_code) END AS person_code,
CASE WHEN trim(patient_relationship_code) = '' THEN NULL ELSE trim(patient_relationship_code) END AS patient_relationship_code,
CASE WHEN trim(eligibility_clarification_code) = '' THEN NULL ELSE trim(eligibility_clarification_code) END AS eligibility_clarification_code,
CASE WHEN trim(home_plan) = '' THEN NULL ELSE trim(home_plan) END AS home_plan,
CASE WHEN trim(cob_count) = '' THEN NULL ELSE trim(cob_count) END AS cob_count,
CASE WHEN trim(other_payer_coverage_type) = '' THEN NULL ELSE trim(other_payer_coverage_type) END AS other_payer_coverage_type,
CASE WHEN trim(other_payer_coverage_qual) = '' THEN NULL ELSE trim(other_payer_coverage_qual) END AS other_payer_coverage_qual,
CASE WHEN trim(other_payer_coverage_id) = '' THEN NULL ELSE trim(other_payer_coverage_id) END AS other_payer_coverage_id,
CASE WHEN trim(other_payer_amount_paid_qualifier) = '' THEN NULL ELSE trim(other_payer_amount_paid_qualifier) END AS other_payer_amount_paid_qualifier,
CASE WHEN trim(other_payer_amount_paid_submitted) = '' THEN NULL ELSE trim(other_payer_amount_paid_submitted) END AS other_payer_amount_paid_submitted,
extract_date(other_payer_date, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)) AS other_payer_date,
CASE WHEN trim(carrier_id) = '' THEN NULL ELSE trim(carrier_id) END AS carrier_id,
extract_date(date_injury, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)) AS date_injury,
CASE WHEN trim(claim_reference_id) = '' THEN NULL ELSE trim(claim_reference_id) END AS claim_reference_id,
CASE WHEN trim(other_payer_coverage_code) = '' THEN NULL ELSE trim(other_payer_coverage_code) END AS other_payer_coverage_code,
CASE WHEN trim(rx_number) = '' THEN NULL ELSE trim(rx_number) END AS rx_number,  --internal pharmacy claim number
CASE WHEN trim(fill_number) = '' THEN NULL ELSE trim(fill_number) END AS fill_number,
CASE WHEN trim(days_supply) = '' THEN NULL ELSE trim(days_supply) END AS days_supply,
CASE WHEN trim(compound_code) = '' THEN NULL ELSE trim(compound_code) END AS compound_code,
CASE WHEN trim(product_service_id) = '' THEN NULL ELSE trim(product_service_id) END AS product_service_id,   --ndc code or something else
CASE WHEN trim(dispensed_as_written) = '' THEN NULL ELSE trim(dispensed_as_written) END AS dispensed_as_written,
extract_date(date_written, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)) AS date_written,
CASE WHEN trim(refill_auth_amount) = '' THEN NULL ELSE trim(refill_auth_amount) END AS refill_auth_amount,
CASE WHEN trim(level_of_service) = '' THEN NULL ELSE trim(level_of_service) END AS level_of_service,
CASE WHEN trim(prescription_origin) = '' THEN NULL ELSE trim(prescription_origin) END AS prescription_origin,
CASE WHEN trim(submission_clarification) = '' THEN NULL ELSE trim(submission_clarification) END AS submission_clarification,
CASE WHEN trim(unit_dose_indicator) = '' THEN NULL ELSE trim(unit_dose_indicator) END AS unit_dose_indicator,
CASE WHEN trim(product_service_id_qualifier) = '' THEN NULL ELSE trim(product_service_id_qualifier) END AS product_service_id_qualifier,   --will indicate what kind of ProductServiceID it is  "03" indicates NDC code
CASE WHEN trim(dispensed_quantity) = '' THEN NULL ELSE trim(dispensed_quantity) END AS dispensed_quantity,
CASE WHEN trim(orig_prescribed_product_service_code) = '' THEN NULL ELSE trim(orig_prescribed_product_service_code) END AS orig_prescribed_product_service_code,
CASE WHEN trim(orig_prescribed_quantity) = '' THEN NULL ELSE trim(orig_prescribed_quantity) END AS orig_prescribed_quantity,
CASE WHEN trim(orig_prescribed_product_service_code_qual) = '' THEN NULL ELSE trim(orig_prescribed_product_service_code_qual) END AS orig_prescribed_product_service_code_qual,
CASE WHEN trim(rx_number_qual) = '' THEN NULL ELSE trim(rx_number_qual) END AS rx_number_qual,
CASE WHEN trim(prior_auth_type_code) = '' THEN NULL ELSE trim(prior_auth_type_code) END AS prior_auth_type_code,
CASE WHEN trim(unit_of_measure) = '' THEN NULL ELSE trim(unit_of_measure) END AS unit_of_measure,
CASE WHEN trim(reason_for_service) = '' THEN NULL ELSE trim(reason_for_service) END AS reason_for_service,
CASE WHEN trim(professional_service_code) = '' THEN NULL ELSE trim(professional_service_code) END AS professional_service_code,
CASE WHEN trim(result_of_service_code) = '' THEN NULL ELSE trim(result_of_service_code) END AS result_of_service_code,
CASE WHEN trim(coupon_type) = '' THEN NULL ELSE trim(coupon_type) END AS coupon_type,
CASE WHEN trim(coupon_number) = '' THEN NULL ELSE trim(coupon_number) END AS coupon_number,
CASE WHEN trim(coupon_value) = '' THEN NULL ELSE trim(coupon_value) END AS coupon_value,
CASE WHEN trim(submitted_ingredient_cost) = '' THEN NULL ELSE trim(submitted_ingredient_cost) END AS submitted_ingredient_cost,
CASE WHEN trim(submitted_dispensing_fee) = '' THEN NULL ELSE trim(submitted_dispensing_fee) END AS submitted_dispensing_fee,
CASE WHEN trim(basis_of_cost_determination) = '' THEN NULL ELSE trim(basis_of_cost_determination) END AS basis_of_cost_determination,
CASE WHEN trim(usual_and_customary_charge) = '' THEN NULL ELSE trim(usual_and_customary_charge) END AS usual_and_customary_charge,
CASE WHEN trim(submitted_patient_pay) = '' THEN NULL ELSE trim(submitted_patient_pay) END AS submitted_patient_pay,
CASE WHEN trim(submitted_gross_due) = '' THEN NULL ELSE trim(submitted_gross_due) END AS submitted_gross_due,
CASE WHEN trim(submitted_incentive) = '' THEN NULL ELSE trim(submitted_incentive) END AS submitted_incentive,
CASE WHEN trim(submitted_professional_service_fee) = '' THEN NULL ELSE trim(submitted_professional_service_fee) END AS submitted_professional_service_fee,
CASE WHEN trim(submitted_other_claimed_qual) = '' THEN NULL ELSE trim(submitted_other_claimed_qual) END AS submitted_other_claimed_qual,
CASE WHEN trim(submitted_other_claimed) = '' THEN NULL ELSE trim(submitted_other_claimed) END AS submitted_other_claimed,
CASE WHEN trim(submitted_flat_sales_tax) = '' THEN NULL ELSE trim(submitted_flat_sales_tax) END AS submitted_flat_sales_tax,
CASE WHEN trim(submitted_percent_sales_tax_amount) = '' THEN NULL ELSE trim(submitted_percent_sales_tax_amount) END AS submitted_percent_sales_tax_amount,
CASE WHEN trim(submitted_percent_sales_tax_rate) = '' THEN NULL ELSE trim(submitted_percent_sales_tax_rate) END AS submitted_percent_sales_tax_rate,
CASE WHEN trim(submitted_percent_sales_tax_basis) = '' THEN NULL ELSE trim(submitted_percent_sales_tax_basis) END AS submitted_percent_sales_tax_basis,
CASE WHEN trim(diagnosis_code) = '' THEN NULL ELSE trim(diagnosis_code) END AS diagnosis_code,
CASE WHEN trim(diagnosis_code_qual) = '' THEN NULL ELSE trim(diagnosis_code_qual) END AS diagnosis_code_qual,
CASE WHEN trim(response_code) = '' THEN NULL ELSE trim(response_code) END AS response_code,
CASE WHEN trim(paid_patient_pay) = '' THEN NULL ELSE trim(paid_patient_pay) END AS paid_patient_pay,
CASE WHEN trim(paid_ingredient_cost) = '' THEN NULL ELSE trim(paid_ingredient_cost) END AS paid_ingredient_cost,
CASE WHEN trim(paid_dispensing_fee) = '' THEN NULL ELSE trim(paid_dispensing_fee) END AS paid_dispensing_fee,
CASE WHEN trim(paid_gross_due) = '' THEN NULL ELSE trim(paid_gross_due) END AS paid_gross_due,
CASE WHEN trim(accumulated_deductible) = '' THEN NULL ELSE trim(accumulated_deductible) END AS accumulated_deductible,
CASE WHEN trim(remaining_deductible) = '' THEN NULL ELSE trim(remaining_deductible) END AS remaining_deductible,
CASE WHEN trim(remaining_benefit) = '' THEN NULL ELSE trim(remaining_benefit) END AS remaining_benefit,
CASE WHEN trim(periodic_deductible_applied) = '' THEN NULL ELSE trim(periodic_deductible_applied) END AS periodic_deductible_applied,
CASE WHEN trim(copay_coinsurance) = '' THEN NULL ELSE trim(copay_coinsurance) END AS copay_coinsurance,
CASE WHEN trim(product_selection_attributed) = '' THEN NULL ELSE trim(product_selection_attributed) END AS product_selection_attributed,
CASE WHEN trim(periodic_benefit_exceed) = '' THEN NULL ELSE trim(periodic_benefit_exceed) END AS periodic_benefit_exceed,
CASE WHEN trim(incentive_paid) = '' THEN NULL ELSE trim(incentive_paid) END AS incentive_paid,
CASE WHEN trim(basis_of_reimbursement_determination) = '' THEN NULL ELSE trim(basis_of_reimbursement_determination) END AS basis_of_reimbursement_determination,
CASE WHEN trim(sales_tax) = '' THEN NULL ELSE trim(sales_tax) END AS sales_tax,
CASE WHEN trim(tax_exempt_indicator) = '' THEN NULL ELSE trim(tax_exempt_indicator) END AS tax_exempt_indicator,
CASE WHEN trim(paid_flat_sales_tax) = '' THEN NULL ELSE trim(paid_flat_sales_tax) END AS paid_flat_sales_tax,
CASE WHEN trim(paid_percentage_sales_tax_amount) = '' THEN NULL ELSE trim(paid_percentage_sales_tax_amount) END AS paid_percentage_sales_tax_amount,
CASE WHEN trim(paid_percent_sales_tax_rate) = '' THEN NULL ELSE trim(paid_percent_sales_tax_rate) END AS paid_percent_sales_tax_rate,
CASE WHEN trim(paid_percent_sales_tax_basis) = '' THEN NULL ELSE trim(paid_percent_sales_tax_basis) END AS paid_percent_sales_tax_basis,
CASE WHEN trim(paid_professional_service_fee) = '' THEN NULL ELSE trim(paid_professional_service_fee) END AS paid_professional_service_fee,
CASE WHEN trim(paid_other_claimed_qual) = '' THEN NULL ELSE trim(paid_other_claimed_qual) END AS paid_other_claimed_qual,
CASE WHEN trim(paid_other_claimed) = '' THEN NULL ELSE trim(paid_other_claimed) END AS paid_other_claimed,
CASE WHEN trim(other_payer_recognized) = '' THEN NULL ELSE trim(other_payer_recognized) END AS other_payer_recognized,
CASE WHEN trim(payer_plan_id) = '' THEN NULL ELSE trim(payer_plan_id) END AS payer_plan_id,
CASE WHEN trim(ncpdp_number) = '' THEN NULL ELSE trim(ncpdp_number) END AS ncpdp_number,
CASE WHEN trim(pharmacy_npi) = '' THEN NULL ELSE trim(pharmacy_npi) END AS pharmacy_npi,
CASE WHEN trim(plan_type) = '' THEN NULL ELSE trim(plan_type) END AS plan_type,
CASE WHEN trim(pharmacy_postal_code) = '' THEN NULL ELSE trim(pharmacy_postal_code) END AS pharmacy_postal_code,
CASE WHEN trim(reject_reason_code_1) = '' THEN NULL ELSE trim(reject_reason_code_1) END AS reject_reason_code_1,
CASE WHEN trim(reject_reason_code_2) = '' THEN NULL ELSE trim(reject_reason_code_2) END AS reject_reason_code_2,
CASE WHEN trim(reject_reason_code_3) = '' THEN NULL ELSE trim(reject_reason_code_3) END AS reject_reason_code_3,
CASE WHEN trim(reject_reason_code_4) = '' THEN NULL ELSE trim(reject_reason_code_4) END AS reject_reason_code_4,
CASE WHEN trim(reject_reason_code_5) = '' THEN NULL ELSE trim(reject_reason_code_5) END AS reject_reason_code_5,
CASE WHEN trim(payer_id) = '' THEN NULL ELSE trim(payer_id) END AS payer_id,
CASE WHEN trim(payer_id_qual) = '' THEN NULL ELSE trim(payer_id_qual) END AS payer_id_qual,
CASE WHEN trim(payer_plan_name) = '' THEN NULL ELSE trim(payer_plan_name) END AS payer_plan_name,
CASE WHEN trim(payer_type) = '' THEN NULL ELSE trim(payer_type) END AS payer_type,
CASE WHEN trim(claim_id) = '' THEN NULL ELSE trim(claim_id) END AS claim_id
FROM emdeon_rx_raw;
