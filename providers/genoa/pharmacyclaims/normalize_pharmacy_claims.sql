INSERT INTO pharmacyclaims_common_model (
claim_id,
hvid,
patient_gender,
patient_year_of_birth,
patient_zip3,
patient_state,
date_service,
transaction_code_std,
response_code_std,
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
payer_id,
payer_id_qual,
payer_plan_id,
payer_plan_name,
compound_code,
prov_prescribing_npi,
copay_coinsurance,
paid_ingredient_cost,
paid_dispensing_fee,
paid_gross_due,
pharmacy_other_id,
pharmacy_other_qual,
pharmacy_postal_code,
prov_prescribing_id,
prov_prescribing_qual)
SELECT
sales_key,
hvid,
CASE WHEN UPPER(gender) = 'M' OR UPPER(patient_gender) = 'M' THEN 'M' WHEN UPPER(gender) = 'F' OR UPPER(patient_gender) = 'M' THEN 'F' ELSE 'U' END,
yearofbirth,
threeDigitZip,
state,
date_of_service,
transaction_code,
response_code,
CASE WHEN ltrim(product_service_id_qualifier) in ('7','8','9','07','08','09') then ltrim(product_service_id) else NULL END as procedure_code,
CASE WHEN ltrim(product_service_id_qualifier) in ('7','8','9','07','08','09') then ltrim(product_service_id_qualifier) else NULL END as procedure_code_qual,
CASE WHEN ltrim(product_service_id_qualifier) in ('3','03') then ltrim(product_service_id) else NULL END as ndc_code,
CASE WHEN ltrim(product_service_id_qualifier) not in ('7','8','9','07','08','09','3','03') then ltrim(product_service_id) else NULL end as product_service_id,
CASE WHEN ltrim(product_service_id_qualifier) not in ('7','8','9','07','08','09','3','03') then ltrim(product_service_id_qualifier) else NULL end as product_service_id_qual,
ltrim(prescription_service_reference_number),
ltrim(prescription_service_reference_number_qualifier),
ltrim(bin_number),
ltrim(processor_control_number),
CASE WHEN (length(fill_number)-length(replace(fill_number,'.',''))) = 1 THEN
('0' || regexp_replace(fill_number, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(fill_number, '[^0-9]'))::bigint::text END,
ltrim(number_of_refills_authorized),
CASE WHEN (length(quantity_dispensed)-length(replace(quantity_dispensed,'.',''))) = 1 THEN
('0' || regexp_replace(quantity_dispensed, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(quantity_dispensed, '[^0-9]'))::bigint::text END,
ltrim(unit_of_measure),
CASE WHEN (length(days_supply)-length(replace(days_supply,'.',''))) = 1 THEN
('0' || regexp_replace(days_supply, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(days_supply, '[^0-9]'))::bigint::text END,
CASE WHEN service_provider_id_qualifier in ('1', '01') then service_provider_id else null end,
payer_id,
payer_id_qualifier,
ltrim(plan_identification),
ltrim(plan_name),
ltrim(compound_code),
CASE WHEN (ltrim(prescriber_id_qualifier) in ('P')) then ltrim(prescriber_id) else NULL end as prov_prescribing_npi,
CASE WHEN (length(amount_of_copay_coinsurance)-length(replace(amount_of_copay_coinsurance,'.',''))) = 1 THEN
('0' || regexp_replace(amount_of_copay_coinsurance, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(amount_of_copay_coinsurance, '[^0-9]'))::bigint::text END,
CASE WHEN (length(ingredient_cost_paid)-length(replace(ingredient_cost_paid,'.',''))) = 1 THEN
('0' || regexp_replace(ingredient_cost_paid, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(ingredient_cost_paid, '[^0-9]'))::bigint::text END,
CASE WHEN (length(dispensing_fee_paid)-length(replace(dispensing_fee_paid,'.',''))) = 1 THEN
('0' || regexp_replace(dispensing_fee_paid, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(dispensing_fee_paid, '[^0-9]'))::bigint::text END,
CASE WHEN (length(total_amount_paid)-length(replace(total_amount_paid,'.',''))) = 1 THEN
('0' || regexp_replace(total_amount_paid, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(total_amount_paid, '[^0-9]'))::bigint::text END,
CASE WHEN service_provider_id_qualifier not in ('1', '01') then service_provider_id else null end,
CASE WHEN service_provider_id_qualifier not in ('1', '01') then service_provider_id_qualifier else null end,
ltrim(pharmacy_location__postal_code_),
CASE WHEN (ltrim(prescriber_id_qualifier) not in ('P')) then ltrim(prescriber_id) else NULL end as prov_prescribing_id,
CASE WHEN (ltrim(prescriber_id_qualifier) not in ('P')) then ltrim(prescriber_id_qualifier) else NULL end as prescriber_id_qualifier
FROM genoa_rx_raw
    LEFT JOIN matching_payload ON hv_join_key = hvjoinkey
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3;

UPDATE pharmacyclaims_common_model
SET date_service = dates.formatted
FROM dates
WHERE date_service = dates.date;

UPDATE pharmacyclaims_common_model SET
date_service = CASE WHEN length(date_service) <> 10 THEN NULL ELSE date_service END;

UPDATE pharmacyclaims_common_model SET patient_year_of_birth=NULL
WHERE
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
date_service IS NULL OR (patient_year_of_birth < (extract('year' from date_service::date - '32873 days'::interval)::text)) OR patient_year_of_birth > (extract('year' from getdate())::text);
