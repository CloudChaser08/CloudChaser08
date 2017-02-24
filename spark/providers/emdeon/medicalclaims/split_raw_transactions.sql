-- Create a table for medical claim data
DROP TABLE IF EXISTS emdeon_dx_raw_claims;
CREATE TABLE emdeon_dx_raw_claims (
claim_id string,
payer_vendor_id string,
diagnosis_code_qual string,
date_received string,
claim_type string,
contract_allow_ind string,
payer_name string,
sub_client_id string,
group_name string,
member_id string,
member_fname string,
member_lname string,
member_gender string,
member_dob string,
member_adr_line1 string,
member_adr_line2 string,
member_adr_city string,
member_adr_state string,
member_adr_zip string,
patient_id string,
patient_relation string,
patient_fname string,
patient_lname string,
patient_gender string,
patient_dob string,
patient_age string,
prov_billing_tax_id string,
prov_billing_npi string,
prov_billing_name_1 string,
prov_billing_name_2 string,
prov_billing_address_1 string,
prov_billing_address_2 string,
prov_billing_city string,
prov_billing_state string,
prov_billing_zip string,
prov_referring_tax_id string,
prov_referring_npi string,
prov_referring_name_1 string,
prov_referring_name_2 string,
prov_rendering_tax_id string,
prov_rendering_npi string,
prov_rendering_name_1 string,
prov_rendering_name_2 string,
prov_facility_tax_id string,
prov_facility_name_1 string,
prov_facility_name_2 string,
prov_facility_address_1 string,
prov_facility_address_2 string,
prov_facility_city string,
prov_facility_state string,
prov_facility_zip string,
statement_from string,
statement_to string,
total_charge string,
total_allowed string,
inst_drg_std_id string,
patient_control string,
inst_type_of_bill_std_id string,
release_sign string,
assignment_sign string,
in_out_network string,
principal_procedure string,
admit_diagnosis string,
primary_diagnosis string,
diagnosis_code_2 string,
diagnosis_code_3 string,
diagnosis_code_4 string,
diagnosis_code_5 string,
diagnosis_code_6 string,
diagnosis_code_7 string,
diagnosis_code_8 string,
other_proc_code_2 string,
other_proc_code_3 string,
other_proc_code_4 string,
other_proc_code_5 string,
other_proc_code_6 string,
prov_specialty string,
medical_coverage_type string,
explanation_code string,
accident_related string,
esrd_patient string,
hosp_admis_or_er string,
amb_nurse_to_hosp string,
not_covrd_specialt string,
electronic_claim string,
dialysis_related string,
new_patient string,
initial_procedure string,
amb_nurse_to_diag string,
amb_hosp_to_hosp string,
inst_date_admitted string,
admission_hour string,
inst_admit_type_std_id string,
inst_admit_source_std_id string,
discharge_hour string,
inst_discharge_status_std_id string,
tooth_number string,
other_proc_code_7 string,
other_proc_code_8 string,
other_proc_code_9 string,
other_proc_code_10 string,
prov_billing_std_taxonomy string,
prov_billing_state_license string,
prov_billing_upin string,
prov_billing_ssn string,
prov_rendering_std_taxonomy string,
prov_rendering_state_license string,
prov_rendering_upin string,
prov_facility_npi string,
prov_facility_state_license string
);

-- Create a table for medical service data
DROP TABLE IF EXISTS emdeon_dx_raw_service;
CREATE TABLE emdeon_dx_raw_service (
claim_id string,
service_line_number string,
date_service string,
date_service_end string,
place_of_service_std_id string,
procedure_code string,
procedure_code_qual string,
procedure_modifier_1 string,
procedure_modifier_2 string,
procedure_modifier_3 string,
procedure_modifier_4 string,
line_charge string,
line_allowed string,
procedure_units string,
revenue_code string,
diagnosis_pointer_1 string,
diagnosis_pointer_2 string,
diagnosis_pointer_3 string,
diagnosis_pointer_4 string,
diagnosis_pointer_5 string,
diagnosis_pointer_6 string,
diagnosis_pointer_7 string,
diagnosis_pointer_8 string,
ndc_code string,
ambulance_to_hosp string,
emergency string,
tooth_surface string,
oral_cavity string,
type_service string,
copay string,
paid_amount string,
date_paid string,
bene_not_entitled string,
patient_reach_max string,
svc_during_postop string,
adjudicated_procedure string,
adjudicated_procedure_qual string,
adjudicated_proc_modifier_1 string,
adjudicated_proc_modifier_2 string,
adjudicated_proc_modifier_3 string,
adjudicated_proc_modifier_4 string
);

-- Create a table for medical diagnosis data
DROP TABLE IF EXISTS emdeon_dx_raw_diagnosis;
CREATE TABLE emdeon_dx_raw_diagnosis (
claim_id string,
diagnosis_code_9 string,
diagnosis_code_10 string,
diagnosis_code_11 string,
diagnosis_code_12 string,
diagnosis_code_13 string,
diagnosis_code_14 string,
diagnosis_code_15 string,
diagnosis_code_16 string,
diagnosis_code_17 string,
diagnosis_code_18 string,
diagnosis_code_19 string,
diagnosis_code_20 string,
diagnosis_code_21 string,
diagnosis_code_22 string,
diagnosis_code_23 string,
diagnosis_code_24 string,
diagnosis_code_25 string,
other_proc_code_11 string,
other_proc_code_12 string,
other_proc_code_13 string,
other_proc_code_14 string,
other_proc_code_15 string,
other_proc_code_16 string,
other_proc_code_17 string,
other_proc_code_18 string,
other_proc_code_19 string,
other_proc_code_20 string,
other_proc_code_21 string,
other_proc_code_22 string,
other_proc_code_23 string,
other_proc_code_24 string,
other_proc_code_25 string,
claim_filing_ind_cd string,
prov_referring_state_license string,
prov_referring_upin string,
prov_referring_commercial_id string,
pay_to_prov_address_1 string,
pay_to_prov_address_2 string,
pay_to_prov_city string,
pay_to_prov_zip string,
pay_to_prov_state string,
supervising_pr_org_name string,
supervising_pr_last_name string,
supervising_pr_first_name string,
supervising_pr_middle_name string,
supervising_pr_npi string,
supervising_pr_state_lic string,
supervising_pr_upin string,
supervising_pr_commercial string,
supervising_pr_location string,
operating_pr_org_name string,
operating_pr_last_name string,
operating_pr_first_name string,
operating_pr_middle_name string,
operating_pr_npi string,
operating_pr_state_lic string,
operating_pr_upin string,
operating_pr_commercial string,
operating_pr_location string,
oth_operating_pr_org_name string,
oth_operating_pr_last_name string,
oth_operating_pr_first_name string,
oth_operating_pr_middle_name string,
oth_operating_pr_npi string,
oth_operating_pr_state_lic string,
oth_operating_pr_upin string,
oth_operating_pr_commercial string,
oth_operating_pr_location string,
pay_to_plan_name string,
pay_to_plan_address_1 string,
pay_to_plan_address_2 string,
pay_to_plan_city string,
pay_to_plan_zip string,
pay_to_plan_state string,
pay_to_plan_naic_id string,
pay_to_plan_payer_id string,
pay_to_plan_plan_id string,
pay_to_plan_clm_ofc_number string,
pay_to_plan_tax_id string,
cob_payer_name_1 string,
cob_payer_id_1 string,
cob_payer_hpid_1 string,
cob_payer_seq_code_1 string,
cob_relationship_cd_1 string,
cob_group_policy_nbr_1 string,
cob_group_name_1 string,
cob_ins_type_code_1 string,
cob_payer_claim_filing_ind_code_1 string,
cob_payer_name_2 string,
cob_payer_id_2 string,
cob_payer_hpid_2 string,
cob_payer_seq_code_2 string,
cob_relationship_cd_2 string,
cob_group_policy_nbr_2 string,
cob_group_name_2 string,
cob_ins_type_code_2 string,
cob_payer_claim_filing_ind_code_2 string
);

-- Select medical claim data (column2 = 'C') from the transactions table and insert the first 111 columns into the claims table. A claim row only consists of 111 columns
-- Drop column2 (record type), we won't need it anymore
-- Normalize gender in column 25 (patient_gender in claims) to 'M', 'F', or 'U'
-- Normalize numeric columns (column55 => total_charge, column56 => total_allowed)
-- Normalize date columns (column5 => date_received, column53 => statement_from, column54 => statement_to, column92 => inst_date_admitted)
INSERT INTO emdeon_dx_raw_claims SELECT
trim(column1) as column1,
trim(column3) as column3,
trim(column4) as column4,
extract_date(column5, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)),
trim(column6) as column6,
trim(column7) as column7,
trim(column8) as column8,
trim(column9) as column9,
trim(column10) as column10,
trim(column11) as column11,
trim(column12) as column12,
trim(column13) as column13,
trim(column14) as column14,
trim(column15) as column15,
trim(column16) as column16,
trim(column17) as column17,
trim(column18) as column18,
trim(column19) as column19,
trim(column20) as column20,
trim(column21) as column21,
trim(column22) as column22,
trim(column23) as column23,
trim(column24) as column24,
CASE WHEN UPPER(column25) = 'M' OR column25 = '1' THEN 'M' WHEN UPPER(column25) = 'F' OR column25 = '2' THEN 'F' ELSE 'U' END,
trim(column26) as column26,
trim(column27) as column27,
trim(column28) as column28,
trim(column29) as column29,
trim(column30) as column30,
trim(column31) as column31,
trim(column32) as column32,
trim(column33) as column33,
trim(column34) as column34,
trim(column35) as column35,
trim(column36) as column36,
trim(column37) as column37,
trim(column38) as column38,
trim(column39) as column39,
trim(column40) as column40,
trim(column41) as column41,
trim(column42) as column42,
trim(column43) as column43,
trim(column44) as column44,
trim(column45) as column45,
trim(column46) as column46,
trim(column47) as column47,
trim(column48) as column48,
trim(column49) as column49,
trim(column50) as column50,
trim(column51) as column51,
trim(column52) as column52,
extract_date(column53, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)),
extract_date(column54, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)),
extract_number(column55),
extract_number(column56),
trim(column57) as column57,
trim(column58) as column58,
trim(column59) as column59,
trim(column60) as column60,
trim(column61) as column61,
trim(column62) as column62,
trim(column63) as column63,
trim(column64) as column64,
trim(column65) as column65,
trim(column66) as column66,
trim(column67) as column67,
trim(column68) as column68,
trim(column69) as column69,
trim(column70) as column70,
trim(column71) as column71,
trim(column72) as column72,
trim(column73) as column73,
trim(column74) as column74,
trim(column75) as column75,
trim(column76) as column76,
trim(column77) as column77,
trim(column78) as column78,
trim(column79) as column79,
trim(column80) as column80,
trim(column81) as column81,
trim(column82) as column82,
trim(column83) as column83,
trim(column84) as column84,
trim(column85) as column85,
trim(column86) as column86,
trim(column87) as column87,
trim(column88) as column88,
trim(column89) as column89,
trim(column90) as column90,
trim(column91) as column91,
extract_date(column92, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)),
trim(column93) as column93,
trim(column94) as column94,
trim(column95) as column95,
trim(column96) as column96,
trim(column97) as column97,
trim(column98) as column98,
trim(column99) as column99,
trim(column100) as column100,
trim(column101) as column101,
trim(column102) as column102,
trim(column103) as column103,
trim(column104) as column104,
trim(column105) as column105,
trim(column106) as column106,
trim(column107) as column107,
trim(column108) as column108,
trim(column109) as column109,
trim(column110) as column110,
trim(column111) as column111
FROM emdeon_dx_raw_local
WHERE part_record_type = 'C'
CLUSTER BY column1;

-- Select medical service data (column2 = 'S') from the transactions table and insert the first 42 columns into the services table. A service row only consists of 42 columns
-- Drop column2 (record type), we won't need it anymore
-- Normalize numeric columns (column13 => line_charge, column14 => line_allowed, column15 => procedure_units)
-- Normalize date columns (column4 => date_service, column5 => date_service_ends)
INSERT INTO emdeon_dx_raw_service SELECT
trim(column1) as column1,
trim(column3) as column3,
extract_date(column4, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)),
extract_date(column5, '%Y%m%d', cast({min_date} as date), cast({max_date} as date)),
trim(column6) as column6,
trim(column7) as column7,
trim(column8) as column8,
trim(column9) as column9,
trim(column10) as column10,
trim(column11) as column11,
trim(column12) as column12,
extract_number(column13),
extract_number(column14),
extract_number(column15),
trim(column16) as column16,
trim(column17) as column17,
trim(column18) as column18,
trim(column19) as column19,
trim(column20) as column20,
trim(column21) as column21,
trim(column22) as column22,
trim(column23) as column23,
trim(column24) as column24,
trim(column25) as column25,
trim(column26) as column26,
trim(column27) as column27,
trim(column28) as column28,
trim(column29) as column29,
trim(column30) as column30,
trim(column31) as column31,
trim(column32) as column32,
trim(column33) as column33,
trim(column34) as column34,
trim(column35) as column35,
trim(column36) as column36,
trim(column37) as column37,
trim(column38) as column38,
trim(column39) as column39,
trim(column40) as column40,
trim(column41) as column41,
trim(column42) as column42
FROM emdeon_dx_raw_local
WHERE part_record_type = 'S'
CLUSTER BY column1;
    

-- Select medical diagnosis data (column2 = 'D') from the transactions table and insert the first 99 columns into the diagnoses table. A diagnosis row consists of all 171 columns, but only the first 99 are relevant for us
-- Drop column2 (record type), we won't need it anymore
INSERT INTO emdeon_dx_raw_diagnosis SELECT
trim(column1) as column1,
trim(column3) as column3,
trim(column4) as column4,
trim(column5) as column5,
trim(column6) as column6,
trim(column7) as column7,
trim(column8) as column8,
trim(column9) as column9,
trim(column10) as column10,
trim(column11) as column11,
trim(column12) as column12,
trim(column13) as column13,
trim(column14) as column14,
trim(column15) as column15,
trim(column16) as column16,
trim(column17) as column17,
trim(column18) as column18,
trim(column19) as column19,
trim(column20) as column20,
trim(column21) as column21,
trim(column22) as column22,
trim(column23) as column23,
trim(column24) as column24,
trim(column25) as column25,
trim(column26) as column26,
trim(column27) as column27,
trim(column28) as column28,
trim(column29) as column29,
trim(column30) as column30,
trim(column31) as column31,
trim(column32) as column32,
trim(column33) as column33,
trim(column34) as column34,
trim(column35) as column35,
trim(column36) as column36,
trim(column37) as column37,
trim(column38) as column38,
trim(column39) as column39,
trim(column40) as column40,
trim(column41) as column41,
trim(column42) as column42,
trim(column43) as column43,
trim(column44) as column44,
trim(column45) as column45,
trim(column46) as column46,
trim(column47) as column47,
trim(column48) as column48,
trim(column49) as column49,
trim(column50) as column50,
trim(column51) as column51,
trim(column52) as column52,
trim(column53) as column53,
trim(column54) as column54,
trim(column55) as column55,
trim(column56) as column56,
trim(column57) as column57,
trim(column58) as column58,
trim(column59) as column59,
trim(column60) as column60,
trim(column61) as column61,
trim(column62) as column62,
trim(column63) as column63,
trim(column64) as column64,
trim(column65) as column65,
trim(column66) as column66,
trim(column67) as column67,
trim(column68) as column68,
trim(column69) as column69,
trim(column70) as column70,
trim(column71) as column71,
trim(column72) as column72,
trim(column73) as column73,
trim(column74) as column74,
trim(column75) as column75,
trim(column76) as column76,
trim(column77) as column77,
trim(column78) as column78,
trim(column79) as column79,
trim(column80) as column80,
trim(column81) as column81,
trim(column82) as column82,
trim(column83) as column83,
trim(column84) as column84,
trim(column85) as column85,
trim(column86) as column86,
trim(column87) as column87,
trim(column88) as column88,
trim(column89) as column89,
trim(column90) as column90,
trim(column91) as column91,
trim(column92) as column92,
trim(column93) as column93,
trim(column94) as column94,
trim(column95) as column95,
trim(column96) as column96,
trim(column97) as column97,
trim(column98) as column98,
trim(column99) as column99
FROM emdeon_dx_raw_local
WHERE part_record_type = 'D'
CLUSTER BY column1;
