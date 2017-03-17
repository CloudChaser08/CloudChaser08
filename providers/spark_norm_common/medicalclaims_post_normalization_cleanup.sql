DROP TABLE IF EXISTS medicalclaims_cleanup;
CREATE TABLE medicalclaims_cleanup AS
SELECT
monotonically_increasing_id() AS record_id,
claim_id,
hvid,
created,
model_version,
data_set,
data_feed,
data_vendor,
source_version,
patient_gender,
-- The number of people over 85 years old is small enough that along with other public fields,
-- age poses re-identification risk, top cap it to 90
CASE WHEN cast(patient_age as int) > 85 THEN '90' ELSE patient_age END as patient_age,
patient_year_of_birth,
patient_zip3,
patient_state,
claim_type,
date_received,
CASE WHEN date_service < '2012-01-01' THEN NULL ELSE date_service END as date_service,
date_service_end,
inst_date_admitted,
inst_date_discharged,
inst_admit_type_std_id,
inst_admit_type_vendor_id,
inst_admit_type_vendor_desc,
inst_admit_source_std_id,
inst_admit_source_vendor_id,
inst_admit_source_vendor_desc,
-- These statuses are specific enough that along with other public fields they pose a
-- re-identification risk, set them to 0 (unkown value)
CASE WHEN inst_discharge_status_std_id IN ('69', '87') THEN 0 ELSE inst_discharge_status_std_id END as inst_discharge_status_std_id,
inst_discharge_status_vendor_id,
inst_discharge_status_vendor_desc,
inst_type_of_bill_std_id,
inst_type_of_bill_vendor_id,
inst_type_of_bill_vendor_desc,
-- These codes are specific enough that along with other public fields they pose a
-- re-identification risk, nullify them
-- 283 Acute myocardial infarction, expired w/ MCC
-- 284 Acute myocardial infarction, expired w/ CC
-- 285 Acute myocardial infarction, expired w/o CC/MCC 
-- 789 Neonates, died or transferred to another acute care facility
CASE WHEN inst_drg_std_id IN ('283', '284', '285', '789') THEN NULL ELSE inst_drg_std_id END as inst_drg_std_id,
inst_drg_vendor_id,
inst_drg_vendor_desc,
obscure_place_of_service(place_of_service_std_id) as place_of_service_std_id,
place_of_service_vendor_id,
place_of_service_vendor_desc,
service_line_number,
clean_up_diagnosis_code(diagnosis_code, diagnosis_code_qual, date_service) as diagnosis_code,
diagnosis_code_qual,
diagnosis_priority,
admit_diagnosis_ind,
procedure_code,
procedure_code_qual,
principal_proc_ind,
procedure_units,
procedure_modifier_1,
procedure_modifier_2,
procedure_modifier_3,
procedure_modifier_4,
revenue_code,
ndc_code,
medical_coverage_type,
line_charge,
line_allowed,
total_charge,
total_allowed,
filter_due_to_place_of_service(prov_rendering_npi, place_of_service_std_id) as prov_rendering_npi,
filter_due_to_place_of_service(prov_billing_npi, place_of_service_std_id) as prov_billing_npi,
filter_due_to_place_of_service(prov_referring_npi, place_of_service_std_id) as prov_referring_npi,
filter_due_to_place_of_service(prov_facility_npi, place_of_service_std_id) as prov_facility_npi,
payer_vendor_id,
payer_name,
payer_parent_name,
payer_org_name,
payer_plan_id,
payer_plan_name,
payer_type,
filter_due_to_place_of_service(prov_rendering_vendor_id, place_of_service_std_id) as prov_rendering_vendor_id,
filter_due_to_place_of_service(prov_rendering_tax_id, place_of_service_std_id) as prov_rendering_tax_id,
filter_due_to_place_of_service(prov_rendering_dea_id, place_of_service_std_id) as prov_rendering_dea_id,
filter_due_to_place_of_service(prov_rendering_ssn, place_of_service_std_id) as prov_rendering_ssn,
filter_due_to_place_of_service(prov_rendering_state_license, place_of_service_std_id) as prov_rendering_state_license,
filter_due_to_place_of_service(prov_rendering_upin, place_of_service_std_id) as prov_rendering_upin,
filter_due_to_place_of_service(prov_rendering_commercial_id, place_of_service_std_id) as prov_rendering_commercial_id,
filter_due_to_place_of_service(prov_rendering_name_1, place_of_service_std_id) as prov_rendering_name_1,
filter_due_to_place_of_service(prov_rendering_name_2, place_of_service_std_id) as prov_rendering_name_2,
filter_due_to_place_of_service(prov_rendering_address_1, place_of_service_std_id) as prov_rendering_address_1,
filter_due_to_place_of_service(prov_rendering_address_2, place_of_service_std_id) as prov_rendering_address_2,
filter_due_to_place_of_service(prov_rendering_city, place_of_service_std_id) as prov_rendering_city,
filter_due_to_place_of_service(prov_rendering_state, place_of_service_std_id) as prov_rendering_state,
filter_due_to_place_of_service(prov_rendering_zip, place_of_service_std_id) as prov_rendering_zip,
prov_rendering_std_taxonomy,
prov_rendering_vendor_specialty,
filter_due_to_place_of_service(prov_billing_vendor_id, place_of_service_std_id) as prov_billing_vendor_id,
filter_due_to_place_of_service(prov_billing_tax_id, place_of_service_std_id) as prov_billing_tax_id,
filter_due_to_place_of_service(prov_billing_dea_id, place_of_service_std_id) as prov_billing_dea_id,
filter_due_to_place_of_service(prov_billing_ssn, place_of_service_std_id) as prov_billing_ssn,
filter_due_to_place_of_service(prov_billing_state_license, place_of_service_std_id) as prov_billing_state_license,
filter_due_to_place_of_service(prov_billing_upin, place_of_service_std_id) as prov_billing_upin,
filter_due_to_place_of_service(prov_billing_commercial_id, place_of_service_std_id) as prov_billing_commercial_id,
filter_due_to_place_of_service(prov_billing_name_1, place_of_service_std_id) as prov_billing_name_1,
filter_due_to_place_of_service(prov_billing_name_2, place_of_service_std_id) as prov_billing_name_2,
filter_due_to_place_of_service(prov_billing_address_1, place_of_service_std_id) as prov_billing_address_1,
filter_due_to_place_of_service(prov_billing_address_2, place_of_service_std_id) as prov_billing_address_2,
filter_due_to_place_of_service(prov_billing_city, place_of_service_std_id) as prov_billing_city,
filter_due_to_place_of_service(prov_billing_state, place_of_service_std_id) as prov_billing_state,
filter_due_to_place_of_service(prov_billing_zip, place_of_service_std_id) as prov_billing_zip,
prov_billing_std_taxonomy,
prov_billing_vendor_specialty,
filter_due_to_place_of_service(prov_referring_vendor_id, place_of_service_std_id) as prov_referring_vendor_id,
filter_due_to_place_of_service(prov_referring_tax_id, place_of_service_std_id) as prov_referring_tax_id,
filter_due_to_place_of_service(prov_referring_dea_id, place_of_service_std_id) as prov_referring_dea_id,
filter_due_to_place_of_service(prov_referring_ssn, place_of_service_std_id) as prov_referring_ssn,
filter_due_to_place_of_service(prov_referring_state_license, place_of_service_std_id) as prov_referring_state_license,
filter_due_to_place_of_service(prov_referring_upin, place_of_service_std_id) as prov_referring_upin,
filter_due_to_place_of_service(prov_referring_commercial_id, place_of_service_std_id) as prov_referring_commercial_id,
filter_due_to_place_of_service(prov_referring_name_1, place_of_service_std_id) as prov_referring_name_1,
filter_due_to_place_of_service(prov_referring_name_2, place_of_service_std_id) as prov_referring_name_2,
filter_due_to_place_of_service(prov_referring_address_1, place_of_service_std_id) as prov_referring_address_1,
filter_due_to_place_of_service(prov_referring_address_2, place_of_service_std_id) as prov_referring_address_2,
filter_due_to_place_of_service(prov_referring_city, place_of_service_std_id) as prov_referring_city,
filter_due_to_place_of_service(prov_referring_state, place_of_service_std_id) as prov_referring_state,
filter_due_to_place_of_service(prov_referring_zip, place_of_service_std_id) as prov_referring_zip,
prov_referring_std_taxonomy,
prov_referring_vendor_specialty,
filter_due_to_place_of_service(prov_facility_vendor_id, place_of_service_std_id) as prov_facility_vendor_id,
filter_due_to_place_of_service(prov_facility_tax_id, place_of_service_std_id) as prov_facility_tax_id,
filter_due_to_place_of_service(prov_facility_dea_id, place_of_service_std_id) as prov_facility_dea_id,
filter_due_to_place_of_service(prov_facility_ssn, place_of_service_std_id) as prov_facility_ssn,
filter_due_to_place_of_service(prov_facility_state_license, place_of_service_std_id) as prov_facility_state_license,
filter_due_to_place_of_service(prov_facility_upin, place_of_service_std_id) as prov_facility_upin,
filter_due_to_place_of_service(prov_facility_commercial_id, place_of_service_std_id) as prov_facility_commercial_id,
filter_due_to_place_of_service(prov_facility_name_1, place_of_service_std_id) as prov_facility_name_1,
filter_due_to_place_of_service(prov_facility_name_2, place_of_service_std_id) as prov_facility_name_2,
filter_due_to_place_of_service(prov_facility_address_1, place_of_service_std_id) as prov_facility_address_1,
filter_due_to_place_of_service(prov_facility_address_2, place_of_service_std_id) as prov_facility_address_2,
filter_due_to_place_of_service(prov_facility_city, place_of_service_std_id) as prov_facility_city,
filter_due_to_place_of_service(prov_facility_state, place_of_service_std_id) as prov_facility_state,
filter_due_to_place_of_service(prov_facility_zip, place_of_service_std_id) as prov_facility_zip,
prov_facility_std_taxonomy,
prov_facility_vendor_specialty,
cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2
FROM medicalclaims_common_model;

DROP TABLE medicalclaims_common_model;
ALTER TABLE medicalclaims_cleanup RENAME TO medicalclaims_common_model;