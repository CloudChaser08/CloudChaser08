-- Load transaction data into table
copy emdeon_rx_raw from :input_path credentials :credentials BZIP2 EMPTYASNULL TRIMBLANKS FIXEDWIDTH 'record_id:1,date_authorized:8,time_authorized:6,bin_number:6,version_number:2,transaction_code:2,processor_control_number:10,transaction_count:1,service_provider_id:15,service_provider_id_qualifier:2,date_service:8,year_of_birth:8,gender_code:1,patient_location:2,patient_first_name:12,patient_last_name:15,patient_street_address:30,patient_state_province:2,patient_zip3:15,patient_id_qualifier:2,patient_id:20,provider_id:15,prov_dispensing_qual:2,prescriber_id:15,primary_care_provider_id:15,prescriber_last_name:15,prov_prescribing_qual:2,prov_primary_care_qual:2,group_id:15,cardholder_id:20,person_code:3,patient_relationship_code:1,eligibility_clarification_code:1,home_plan:3,cob_count:1,other_payer_coverage_type:2,other_payer_coverage_qual:2,other_payer_coverage_id:10,other_payer_amount_paid_qualifier:2,other_payer_amount_paid_submitted:10,other_payer_date:8,carrier_id:10,date_injury:8,claim_reference_id:30,other_payer_coverage_code:2,rx_number:12,fill_number:2,days_supply:3,compound_code:1,product_service_id:19,dispensed_as_written:1,date_written:8,refill_auth_amount:2,level_of_service:2,prescription_origin:1,submission_clarification:2,unit_dose_indicator:1,product_service_id_qualifier:2,dispensed_quantity:11,orig_prescribed_product_service_code:19,orig_prescribed_quantity:11,orig_prescribed_product_service_code_qual:2,rx_number_qual:1,prior_auth_type_code:2,unit_of_measure:2,reason_for_service:2,professional_service_code:2,result_of_service_code:2,coupon_type:2,coupon_number:15,coupon_value:8,submitted_ingredient_cost:10,submitted_dispensing_fee:10,basis_of_cost_determination:2,usual_and_customary_charge:10,submitted_patient_pay:10,submitted_gross_due:10,submitted_incentive:10,submitted_professional_service_fee:10,submitted_other_claimed_qual:2,submitted_other_claimed:10,submitted_flat_sales_tax:10,submitted_percent_sales_tax_amount:10,submitted_percent_sales_tax_rate:9,submitted_percent_sales_tax_basis:2,diagnosis_code:15,diagnosis_code_qual:2,response_code:1,paid_patient_pay:10,paid_ingredient_cost:10,paid_dispensing_fee:10,paid_gross_due:10,accumulated_deductible:10,remaining_deductible:10,remaining_benefit:10,periodic_deductible_applied:10,copay_coinsurance:10,product_selection_attributed:10,periodic_benefit_exceed:10,incentive_paid:10,basis_of_reimbursement_determination:2,sales_tax:10,tax_exempt_indicator:1,paid_flat_sales_tax:10,paid_percentage_sales_tax_amount:10,paid_percent_sales_tax_rate:9,paid_percent_sales_tax_basis:2,paid_professional_service_fee:10,paid_other_claimed_qual:2,paid_other_claimed:10,other_payer_recognized:10,payer_plan_id:8,ncpdp_number:7,pharmacy_npi:10,plan_type:1,pharmacy_postal_code:5,reject_reason_code_1:3,reject_reason_code_2:3,reject_reason_code_3:3,reject_reason_code_4:3,reject_reason_code_5:3,payer_id:10,payer_id_qual:2,payer_plan_name:80,payer_type:10,claim_id:16';

-- Load matching payload data into table
copy matching_payload from :matching_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/payloadpaths.json';

INSERT INTO final_output (
claim_id,
hvid,
patient_gender,
patient_year_of_birth,
patient_zip3,
patient_state,
date_service,
date_written,
date_injury,
date_authorized,
time_authorized,
transaction_code,
response_code,
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
bin_number,
processor_control_number,
fill_number,
refill_auth_amount,
dispensed_quantity,
unit_of_measure,
days_supply,
pharmacy_npi,
prov_dispensing_npi,
payer_id,
payer_id_qual,
payer_plan_id,
payer_plan_name,
payer_type,
compound_code,
unit_dose_indicator,
dispensed_as_written,
prescription_origin,
submission_clarification,
orig_prescribed_product_service_code,
orig_prescribed_product_service_code_qual,
orig_prescribed_quantity,
prior_auth_type_code,
level_of_service,
reason_for_service,
professional_service_code,
result_of_service_code,
prov_prescribing_npi,
prov_primary_care_npi,
cob_count,
usual_and_customary_charge,
sales_tax,
product_selection_attributed,
other_payer_recognized,
periodic_deductible_applied,
periodic_benefit_exceed,
accumulated_deductible,
remaining_deductible,
remaining_benefit,
copay_coinsurance,
basis_of_cost_determination,
submitted_ingredient_cost,
submitted_dispensing_fee,
submitted_incentive,
submitted_gross_due,
submitted_professional_service_fee,
submitted_flat_sales_tax,
submitted_percent_sales_tax_basis,
submitted_percent_sales_tax_rate,
submitted_percent_sales_tax_amount,
submitted_patient_pay,
submitted_other_claimed_qual,
submitted_other_claimed,
basis_of_reimbursement_determination,
paid_ingredient_cost,
paid_dispensing_fee,
paid_gross_due,
paid_professional_service_fee,
paid_flat_sales_tax,
paid_percent_sales_tax_basis,
paid_percent_sales_tax_rate,
paid_patient_pay,
paid_other_claimed_qual,
paid_other_claimed,
tax_exempt_indicator,
coupon_type,
coupon_number,
coupon_value,
pharmacy_other_id,
pharmacy_other_qual,
pharmacy_postal_code,
prov_dispensing_id,
prov_dispensing_qual,
prov_prescribing_id,
prov_prescribing_qual,
prov_primary_care_id,
prov_primary_care_qual,
other_payer_coverage_type,
other_payer_coverage_id,
other_payer_coverage_qual,
other_payer_date,
other_payer_coverage_code)
SELECT
ltrim(claim_id),
hvid,
CASE WHEN UPPER(gender_code) = 'M' OR gender_code = '1' THEN 'M' WHEN UPPER(gender_code) = 'F' OR gender_code = '2' THEN 'F' ELSE 'U' END,
ltrim(year_of_birth),
threeDigitZip,
state,
CASE WHEN char_length(btrim(date_service, '0')) >= 8 THEN substring(date_service from 1 for 4) || '-' || substring(date_service from 5 for 2) || '-' || substring(date_service from 7 for 2) ELSE NULL END,
CASE WHEN char_length(btrim(date_written, '0')) >= 8 THEN substring(date_written from 1 for 4) || '-' || substring(date_written from 5 for 2) || '-' || substring(date_written from 7 for 2) ELSE NULL END,
CASE WHEN char_length(btrim(date_injury, '0')) >= 8 THEN substring(date_injury from 1 for 4) || '-' || substring(date_injury from 5 for 2) || '-' || substring(date_injury from 7 for 2) ELSE NULL END,
CASE WHEN char_length(btrim(date_authorized, '0')) >= 8 THEN substring(date_authorized from 1 for 4) || '-' || substring(date_authorized from 5 for 2) || '-' || substring(date_authorized from 7 for 2) ELSE NULL END,
ltrim(time_authorized),
ltrim(transaction_code),
ltrim(response_code),
ltrim(reject_reason_code_1),
ltrim(reject_reason_code_2),
ltrim(reject_reason_code_3),
ltrim(reject_reason_code_4),
ltrim(reject_reason_code_5),
ltrim(diagnosis_code),
ltrim(diagnosis_code_qual),
CASE WHEN product_service_id_qualifier in ('7','8','9','07','08','09') then product_service_id else NULL END as procedure_code,
CASE WHEN product_service_id_qualifier in ('7','8','9','07','08','09') then product_service_id_qualifier else NULL END as procedure_code_qual,
CASE WHEN product_service_id_qualifier in ('3','03') then product_service_id else NULL END as ndc_code,
CASE WHEN product_service_id_qualifier not in ('7','8','9','07','08','09','3','03') then product_service_id else NULL end as product_service_id,
CASE WHEN product_service_id_qualifier not in ('7','8','9','07','08','09','3','03') then product_service_id_qualifier else NULL end as product_service_id_qual,
ltrim(rx_number),
ltrim(rx_number_qual),
ltrim(bin_number),
ltrim(processor_control_number),
ltrim(fill_number),
ltrim(refill_auth_amount),
CASE WHEN (length(dispensed_quantity)-length(replace(dispensed_quantity,'.',''))) = 1 THEN
('0' || regexp_replace(dispensed_quantity, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(dispensed_quantity, '[^0-9]'))::int::text END,
ltrim(unit_of_measure),
CASE WHEN (length(days_supply)-length(replace(days_supply,'.',''))) = 1 THEN
('0' || regexp_replace(days_supply, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(days_supply, '[^0-9]'))::int::text END,
ltrim(pharmacy_npi),
CASE WHEN prov_dispensing_qual in ('1','01') then provider_id else NULL end as prov_dispensing_npi,
ltrim(payer_id),
ltrim(payer_id_qual),
ltrim(payer_plan_id),
ltrim(payer_plan_name),
ltrim(payer_type),
ltrim(compound_code),
ltrim(unit_dose_indicator),
ltrim(dispensed_as_written),
ltrim(prescription_origin),
ltrim(submission_clarification),
ltrim(orig_prescribed_product_service_code),
ltrim(orig_prescribed_product_service_code_qual),
CASE WHEN (length(orig_prescribed_quantity)-length(replace(orig_prescribed_quantity,'.',''))) = 1 THEN
('0' || regexp_replace(orig_prescribed_quantity, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(orig_prescribed_quantity, '[^0-9]'))::int::text END,
ltrim(prior_auth_type_code),
ltrim(level_of_service),
ltrim(reason_for_service),
ltrim(professional_service_code),
ltrim(result_of_service_code),
CASE WHEN prov_prescribing_qual in ('1','01') then prescriber_id else NULL end as prov_prescribing_npi,
CASE WHEN prov_primary_care_qual in ('1','01') then primary_care_provider_id else NULL end as prov_primary_care_npi,
ltrim(cob_count),
CASE WHEN (length(usual_and_customary_charge)-length(replace(usual_and_customary_charge,'.',''))) = 1 THEN
('0' || regexp_replace(usual_and_customary_charge, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(usual_and_customary_charge, '[^0-9.]'))::int::text END,
CASE WHEN (length(sales_tax)-length(replace(sales_tax,'.',''))) = 1 THEN
('0' || regexp_replace(sales_tax, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(sales_tax, '[^0-9]'))::int::text END,
CASE WHEN (length(product_selection_attributed)-length(replace(product_selection_attributed,'.',''))) = 1 THEN
('0' || regexp_replace(product_selection_attributed, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(product_selection_attributed, '[^0-9]'))::int::text END,
CASE WHEN (length(other_payer_recognized)-length(replace(other_payer_recognized,'.',''))) = 1 THEN
('0' || regexp_replace(other_payer_recognized, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(other_payer_recognized, '[^0-9]'))::int::text END,
CASE WHEN (length(periodic_deductible_applied)-length(replace(periodic_deductible_applied,'.',''))) = 1 THEN
('0' || regexp_replace(periodic_deductible_applied, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(periodic_deductible_applied, '[^0-9]'))::int::text END,
CASE WHEN (length(periodic_benefit_exceed)-length(replace(periodic_benefit_exceed,'.',''))) = 1 THEN
('0' || regexp_replace(periodic_benefit_exceed, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(periodic_benefit_exceed, '[^0-9]'))::int::text END,
CASE WHEN (length(accumulated_deductible)-length(replace(accumulated_deductible,'.',''))) = 1 THEN
('0' || regexp_replace(accumulated_deductible, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(accumulated_deductible, '[^0-9]'))::int::text END,
CASE WHEN (length(remaining_deductible)-length(replace(remaining_deductible,'.',''))) = 1 THEN
('0' || regexp_replace(remaining_deductible, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(remaining_deductible, '[^0-9]'))::int::text END,
CASE WHEN (length(remaining_benefit)-length(replace(remaining_benefit,'.',''))) = 1 THEN
('0' || regexp_replace(remaining_benefit, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(remaining_benefit, '[^0-9]'))::int::text END,
CASE WHEN (length(copay_coinsurance)-length(replace(copay_coinsurance,'.',''))) = 1 THEN
('0' || regexp_replace(copay_coinsurance, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(copay_coinsurance, '[^0-9]'))::int::text END,
ltrim(basis_of_cost_determination),
CASE WHEN (length(submitted_ingredient_cost)-length(replace(submitted_ingredient_cost,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_ingredient_cost, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_ingredient_cost, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_dispensing_fee)-length(replace(submitted_dispensing_fee,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_dispensing_fee, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_dispensing_fee, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_dispensing_fee)-length(replace(submitted_dispensing_fee,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_dispensing_fee, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_dispensing_fee, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_gross_due)-length(replace(submitted_gross_due,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_gross_due, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_gross_due, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_professional_service_fee)-length(replace(submitted_professional_service_fee,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_professional_service_fee, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_professional_service_fee, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_flat_sales_tax)-length(replace(submitted_flat_sales_tax,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_flat_sales_tax, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_flat_sales_tax, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_percent_sales_tax_basis)-length(replace(submitted_percent_sales_tax_basis,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_percent_sales_tax_basis, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_percent_sales_tax_basis, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_percent_sales_tax_rate)-length(replace(submitted_percent_sales_tax_rate,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_percent_sales_tax_rate, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_percent_sales_tax_rate, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_percent_sales_tax_amount)-length(replace(submitted_percent_sales_tax_amount,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_percent_sales_tax_amount, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_percent_sales_tax_amount, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_patient_pay)-length(replace(submitted_patient_pay,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_patient_pay, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_patient_pay, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_other_claimed_qual)-length(replace(submitted_other_claimed_qual,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_other_claimed_qual, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_other_claimed_qual, '[^0-9]'))::int::text END,
CASE WHEN (length(submitted_other_claimed)-length(replace(submitted_other_claimed,'.',''))) = 1 THEN
('0' || regexp_replace(submitted_other_claimed, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(submitted_other_claimed, '[^0-9]'))::int::text END,
ltrim(basis_of_reimbursement_determination),
CASE WHEN (length(paid_ingredient_cost)-length(replace(paid_ingredient_cost,'.',''))) = 1 THEN
('0' || regexp_replace(paid_ingredient_cost, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_ingredient_cost, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_dispensing_fee)-length(replace(paid_dispensing_fee,'.',''))) = 1 THEN
('0' || regexp_replace(paid_dispensing_fee, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_dispensing_fee, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_gross_due)-length(replace(paid_gross_due,'.',''))) = 1 THEN
('0' || regexp_replace(paid_gross_due, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_gross_due, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_professional_service_fee)-length(replace(paid_professional_service_fee,'.',''))) = 1 THEN
('0' || regexp_replace(paid_professional_service_fee, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_professional_service_fee, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_flat_sales_tax)-length(replace(paid_flat_sales_tax,'.',''))) = 1 THEN
('0' || regexp_replace(paid_flat_sales_tax, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_flat_sales_tax, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_percent_sales_tax_basis)-length(replace(paid_percent_sales_tax_basis,'.',''))) = 1 THEN
('0' || regexp_replace(paid_percent_sales_tax_basis, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_percent_sales_tax_basis, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_percent_sales_tax_rate)-length(replace(paid_percent_sales_tax_rate,'.',''))) = 1 THEN
('0' || regexp_replace(paid_percent_sales_tax_rate, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_percent_sales_tax_rate, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_patient_pay)-length(replace(paid_patient_pay,'.',''))) = 1 THEN
('0' || regexp_replace(paid_patient_pay, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_patient_pay, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_other_claimed_qual)-length(replace(paid_other_claimed_qual,'.',''))) = 1 THEN
('0' || regexp_replace(paid_other_claimed_qual, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_other_claimed_qual, '[^0-9]'))::int::text END,
CASE WHEN (length(paid_other_claimed)-length(replace(paid_other_claimed,'.',''))) = 1 THEN
('0' || regexp_replace(paid_other_claimed, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(paid_other_claimed, '[^0-9]'))::int::text END,
ltrim(tax_exempt_indicator),
ltrim(coupon_type),
ltrim(coupon_number),
ltrim(coupon_value),
CASE WHEN btrim(ncpdp_number) <> '' and ncpdp_number is not null then ncpdp_number else service_provider_id end as pharmacy_other_id,
CASE WHEN btrim(ncpdp_number) <> '' and ncpdp_number is not null then '07' else service_provider_id_qualifier end as pharmacy_other_qual,
ltrim(pharmacy_postal_code),
CASE WHEN prov_dispensing_qual not in ('1','01') then provider_id else NULL end as prov_dispensing_id,
ltrim(prov_dispensing_qual),
CASE WHEN prov_prescribing_qual not in ('1','01') then prescriber_id else NULL end as prov_prescribing_id,
ltrim(prov_prescribing_qual),
CASE WHEN prov_primary_care_qual not in ('1','01') then primary_care_provider_id else NULL end as prov_primary_care_id,
ltrim(prov_primary_care_qual),
ltrim(other_payer_coverage_type),
ltrim(other_payer_coverage_id),
ltrim(other_payer_coverage_qual),
CASE WHEN char_length(btrim(other_payer_date, '0')) >= 8 THEN substring(other_payer_date from 1 for 4) || '-' || substring(other_payer_date from 5 for 2) || '-' || substring(other_payer_date from 7 for 2) ELSE NULL END,
ltrim(other_payer_coverage_code)
FROM emdeon_rx_raw
    LEFT JOIN matching_payload ON ltrim(claim_id) = claimid
    LEFT JOIN zip3_to_state ON threeDigitZip = ltrim(zip3);

UNLOAD('SELECT * FROM final_output') TO :output_path credentials :credentials BZIP2 DELIMITER '|' NULL AS '' ALLOWOVERWRITE;
