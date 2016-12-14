INSERT INTO pharmacyclaims_common_model (
claim_id,
hvid,
patient_gender,
patient_year_of_birth,
patient_zip3,
patient_state,
date_service,
date_written,
transaction_code_vendor,
response_code_vendor,
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
prov_prescribing_qual)
SELECT
ltrim(pharmacy_claim_id),
hvid,
CASE WHEN UPPER(mat.gender) = 'M' THEN 'M' WHEN UPPER(mat.gender) = 'F' THEN 'F' ELSE 'U' END,
mat.yearOfBirth,
threeDigitZip,
state,
dates_service.formatted,
dates_written.formatted,
CASE WHEN ltrim(transaction_code) = 'D' OR ltrim(transaction_code) = 'd' THEN 'D' ELSE NULL END as transaction_code_vendor,
CASE WHEN ltrim(transaction_code) = 'R' OR ltrim(transaction_code) = 'r' THEN 'R' ELSE NULL END as response_code_vendor,
ltrim(upper(replace(replace(replace(diagnosis_code, '.', ''), ',', ''), ' ', ''))),
ltrim(diagnosis_code_qualifier),
CASE WHEN ltrim(product_service_id_qualifier) in ('7','8','9','07','08','09') then ltrim(product_service_id) else NULL END as procedure_code,
CASE WHEN ltrim(product_service_id_qualifier) in ('7','8','9','07','08','09') then ltrim(product_service_id_qualifier) else NULL END as procedure_code_qual,
CASE WHEN ltrim(product_service_id_qualifier) in ('3','03') then ltrim(product_service_id) else NULL END as ndc_code,
CASE WHEN ltrim(product_service_id_qualifier) not in ('7','8','9','07','08','09','3','03') then ltrim(product_service_id) else NULL end as product_service_id,
CASE WHEN ltrim(product_service_id_qualifier) not in ('7','8','9','07','08','09','3','03') then ltrim(product_service_id_qualifier) else NULL end as product_service_id_qual,
ltrim(mat.rxnumber),
ltrim(prescription_service_reference_number_qualifier),
ltrim(processor_control_number),
ltrim(fill_number),
ltrim(number_of_refills_authorized),
CASE WHEN (length(quantity_dispensed)-length(replace(quantity_dispensed,'.',''))) = 1 THEN
('0' || regexp_replace(quantity_dispensed, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(quantity_dispensed, '[^0-9]'))::bigint::text END,
ltrim(unit_of_measure),
CASE WHEN (length(days_supply)-length(replace(days_supply,'.',''))) = 1 THEN
('0' || regexp_replace(days_supply, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(days_supply, '[^0-9]'))::bigint::text END,
CASE WHEN (ltrim(service_provider_id_qualifier) in ('1','01')) OR (ltrim(service_provider_id_qualifier) in ('5', '05') AND regexp_count(ltrim(service_provider_id), '^[0-9]{10}$') = 1) then ltrim(service_provider_id) else NULL end as pharmacy_npi,
ltrim(compound_code),
ltrim(unit_dose_indicator),
ltrim(dispense_as_written),
ltrim(level_of_service),
CASE WHEN (ltrim(prescriber_id_qualifier) in ('1','01')) OR (ltrim(prescriber_id_qualifier) in ('5', '05') AND regexp_count(ltrim(prescriber_id), '^[0-9]{10}$') = 1) then ltrim(prescriber_id) else NULL end as prov_prescribing_npi,
CASE WHEN (ltrim(service_provider_id_qualifier) not in ('1','01')) AND (ltrim(service_provider_id_qualifier) not in ('5', '05') OR regexp_count(ltrim(service_provider_id), '^[0-9]{10}$') = 0) then ltrim(service_provider_id) else NULL end as pharmacy_other_id,
CASE WHEN (ltrim(service_provider_id_qualifier) not in ('1','01')) AND (ltrim(service_provider_id_qualifier) not in ('5', '05') OR regexp_count(ltrim(service_provider_id), '^[0-9]{10}$') = 0) then ltrim(service_provider_id_qualifier) else NULL end as pharmacy_other_qual,
CASE WHEN (ltrim(prescriber_id_qualifier) not in ('1','01')) AND (ltrim(prescriber_id_qualifier) not in ('5', '05') OR regexp_count(ltrim(prescriber_id), '^[0-9]{10}$') = 0) then ltrim(prescriber_id) else NULL end as prov_prescribing_id,
CASE WHEN (ltrim(prescriber_id_qualifier) not in ('1','01')) AND (ltrim(prescriber_id_qualifier) not in ('5', '05') OR regexp_count(ltrim(prescriber_id), '^[0-9]{10}$') = 0) then ltrim(prescriber_id_qualifier) else NULL end as prescribing_id_qual
FROM express_scripts_rx_raw
    LEFT JOIN matching_payload mat ON hv_join_key = mat.hvJoinKey
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
    LEFT JOIN dates dates_service ON date_of_service = dates_service.date
    LEFT JOIN dates dates_written ON date_prescription_written = dates_written.date;

UPDATE pharmacyclaims_common_model SET patient_year_of_birth=NULL
WHERE
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
date_service IS NULL OR (patient_year_of_birth < (extract('year' from date_service::date - '32873 days'::interval)::text)) OR patient_year_of_birth > (extract('year' from getdate())::text);
