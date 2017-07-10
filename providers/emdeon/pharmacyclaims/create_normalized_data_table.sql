DROP TABLE IF EXISTS :table;
CREATE TABLE :table(
record_id int,
claim_id text encode lzo,
hvid text encode lzo,
created date,
model_version text,
data_set text,
data_feed text,
data_vendor text,
source_version text encode lzo,
patient_gender text encode lzo,
patient_age text encode lzo,
patient_year_of_birth text encode lzo,
patient_zip3 text encode lzo,
patient_state text encode lzo,
date_service text encode lzo,
date_written text encode lzo,
date_injury text encode lzo,
date_authorized text encode lzo,
time_authorized text encode lzo,
transaction_code_std text encode lzo,
transaction_code_vendor text encode lzo,
response_code_std text encode lzo,
response_code_vendor text encode lzo,
reject_reason_code_1 text encode lzo,
reject_reason_code_2 text encode lzo,
reject_reason_code_3 text encode lzo,
reject_reason_code_4 text encode lzo,
reject_reason_code_5 text encode lzo,
diagnosis_code text encode lzo,
diagnosis_code_qual text encode lzo,
procedure_code text encode lzo,
procedure_code_qual text encode lzo,
ndc_code text encode lzo,
product_service_id text encode lzo,
product_service_id_qual text encode lzo,
rx_number text encode lzo,
rx_number_qual text encode lzo,
bin_number text encode lzo,
processor_control_number text encode lzo,
fill_number text encode lzo,
refill_auth_amount text encode lzo,
dispensed_quantity text encode lzo,
unit_of_measure text encode lzo,
days_supply text encode lzo,
pharmacy_npi text encode lzo,
prov_dispensing_npi text encode lzo,
payer_id text encode lzo,
payer_id_qual text encode lzo,
payer_name text encode lzo,
payer_parent_name text encode lzo,
payer_org_name text encode lzo,
payer_plan_id text encode lzo,
payer_plan_name text encode lzo,
payer_type text encode lzo,
compound_code text encode lzo,
unit_dose_indicator text encode lzo,
dispensed_as_written text encode lzo,
prescription_origin text encode lzo,
submission_clarification text encode lzo,
orig_prescribed_product_service_code text encode lzo,
orig_prescribed_product_service_code_qual text encode lzo,
orig_prescribed_quantity text encode lzo,
prior_auth_type_code text encode lzo,
level_of_service text encode lzo,
reason_for_service text encode lzo,
professional_service_code text encode lzo,
result_of_service_code text encode lzo,
prov_prescribing_npi text encode lzo,
prov_primary_care_npi text encode lzo,
cob_count text encode lzo,
usual_and_customary_charge text encode lzo,
sales_tax text encode lzo,
product_selection_attributed text encode lzo,
other_payer_recognized text encode lzo,
periodic_deductible_applied text encode lzo,
periodic_benefit_exceed text encode lzo,
accumulated_deductible text encode lzo,
remaining_deductible text encode lzo,
remaining_benefit text encode lzo,
copay_coinsurance text encode lzo,
basis_of_cost_determination text encode lzo,
submitted_ingredient_cost text encode lzo,
submitted_dispensing_fee text encode lzo,
submitted_incentive text encode lzo,
submitted_gross_due text encode lzo,
submitted_professional_service_fee text encode lzo,
submitted_flat_sales_tax text encode lzo,
submitted_percent_sales_tax_basis text encode lzo,
submitted_percent_sales_tax_rate text encode lzo,
submitted_percent_sales_tax_amount text encode lzo,
submitted_patient_pay text encode lzo,
submitted_other_claimed_qual text encode lzo,
submitted_other_claimed text encode lzo,
basis_of_reimbursement_determination text encode lzo,
paid_ingredient_cost text encode lzo,
paid_dispensing_fee text encode lzo,
paid_incentive text encode lzo,
paid_gross_due text encode lzo,
paid_professional_service_fee text encode lzo,
paid_flat_sales_tax text encode lzo,
paid_percent_sales_tax_basis text encode lzo,
paid_percent_sales_tax_rate text encode lzo,
paid_percent_sales_tax text encode lzo,
paid_patient_pay text encode lzo,
paid_other_claimed_qual text encode lzo,
paid_other_claimed text encode lzo,
tax_exempt_indicator text encode lzo,
coupon_type text encode lzo,
coupon_number text encode lzo,
coupon_value text encode lzo,
pharmacy_other_id text encode lzo,
pharmacy_other_qual text encode lzo,
pharmacy_postal_code text encode lzo,
prov_dispensing_id text encode lzo,
prov_dispensing_qual text encode lzo,
prov_prescribing_id text encode lzo,
prov_prescribing_qual text encode lzo,
prov_primary_care_id text encode lzo,
prov_primary_care_qual text encode lzo,
other_payer_coverage_type text encode lzo,
other_payer_coverage_id text encode lzo,
other_payer_coverage_qual text encode lzo,
other_payer_date text encode lzo,
other_payer_coverage_code text encode lzo,
logical_delete_reason text encode lzo) DISTKEY(pharmacy_npi) SORTKEY(pharmacy_npi);
