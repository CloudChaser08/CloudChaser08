-- Select the medical claims common model into a new table
DROP TABLE IF EXISTS medicalclaims_common_model;
CREATE TABLE medicalclaims_common_model(
record_id int,
claim_id string,
hvid string,
created date,
model_version string,
data_set string,
data_feed string,
data_vendor string,
source_version string,
patient_gender string,
patient_age string,
patient_year_of_birth string,
patient_zip3 string,
patient_state string,
claim_type string,
date_received string,
date_service string,
date_service_end string,
inst_date_admitted string,
inst_date_discharged string,
inst_admit_type_std_id string,
inst_admit_type_vendor_id string,
inst_admit_type_vendor_desc string,
inst_admit_source_std_id string,
inst_admit_source_vendor_id string,
inst_admit_source_vendor_desc string,
inst_discharge_status_std_id string,
inst_discharge_status_vendor_id string,
inst_discharge_status_vendor_desc string,
inst_type_of_bill_std_id string,
inst_type_of_bill_vendor_id string,
inst_type_of_bill_vendor_desc string,
inst_drg_std_id string,
inst_drg_vendor_id string,
inst_drg_vendor_desc string,
place_of_service_std_id string,
place_of_service_vendor_id string,
place_of_service_vendor_desc string,
service_line_number string,
diagnosis_code string,
diagnosis_code_qual string,
diagnosis_priority string,
admit_diagnosis_ind string,
procedure_code string,
procedure_code_qual string,
principal_proc_ind string,
procedure_units string,
procedure_modifier_1 string,
procedure_modifier_2 string,
procedure_modifier_3 string,
procedure_modifier_4 string,
revenue_code string,
ndc_code string,
medical_coverage_type string,
line_charge string,
line_allowed string,
total_charge string,
total_allowed string,
prov_rendering_npi string,
prov_billing_npi string,
prov_referring_npi string,
prov_facility_npi string,
payer_vendor_id string,
payer_name string,
payer_parent_name string,
payer_org_name string,
payer_plan_id string,
payer_plan_name string,
payer_type string,
prov_rendering_vendor_id string,
prov_rendering_tax_id string,
prov_rendering_dea_id string,
prov_rendering_ssn string,
prov_rendering_state_license string,
prov_rendering_upin string,
prov_rendering_commercial_id string,
prov_rendering_name_1 string,
prov_rendering_name_2 string,
prov_rendering_address_1 string,
prov_rendering_address_2 string,
prov_rendering_city string,
prov_rendering_state string,
prov_rendering_zip string,
prov_rendering_std_taxonomy string,
prov_rendering_vendor_specialty string,
prov_billing_vendor_id string,
prov_billing_tax_id string,
prov_billing_dea_id string,
prov_billing_ssn string,
prov_billing_state_license string,
prov_billing_upin string,
prov_billing_commercial_id string,
prov_billing_name_1 string,
prov_billing_name_2 string,
prov_billing_address_1 string,
prov_billing_address_2 string,
prov_billing_city string,
prov_billing_state string,
prov_billing_zip string,
prov_billing_std_taxonomy string,
prov_billing_vendor_specialty string,
prov_referring_vendor_id string,
prov_referring_tax_id string,
prov_referring_dea_id string,
prov_referring_ssn string,
prov_referring_state_license string,
prov_referring_upin string,
prov_referring_commercial_id string,
prov_referring_name_1 string,
prov_referring_name_2 string,
prov_referring_address_1 string,
prov_referring_address_2 string,
prov_referring_city string,
prov_referring_state string,
prov_referring_zip string,
prov_referring_std_taxonomy string,
prov_referring_vendor_specialty string,
prov_facility_vendor_id string,
prov_facility_tax_id string,
prov_facility_dea_id string,
prov_facility_ssn string,
prov_facility_state_license string,
prov_facility_upin string,
prov_facility_commercial_id string,
prov_facility_name_1 string,
prov_facility_name_2 string,
prov_facility_address_1 string,
prov_facility_address_2 string,
prov_facility_city string,
prov_facility_state string,
prov_facility_zip string,
prov_facility_std_taxonomy string,
prov_facility_vendor_specialty string,
cob_payer_vendor_id_1 string,
cob_payer_seq_code_1 string,
cob_payer_hpid_1 string,
cob_payer_claim_filing_ind_code_1 string,
cob_ins_type_code_1 string,
cob_payer_vendor_id_2 string,
cob_payer_seq_code_2 string,
cob_payer_hpid_2 string,
cob_payer_claim_filing_ind_code_2 string,
cob_ins_type_code_2 string);

