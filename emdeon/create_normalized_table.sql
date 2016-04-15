CREATE TABLE emdeon_claim_norm (
claim_number TEXT NULL,
payer_id TEXT NULL,
coding_type TEXT NULL,
received_date DATE null,
claim_type_code TEXT NULL,
contract_allow_ind TEXT NULL,
payer_name TEXT NULL,
sub_client_id TEXT NULL,
group_name TEXT NULL,
member_id TEXT NULL,
member_fname TEXT NULL,
member_lname TEXT NULL,
member_gender TEXT NULL,
member_dob TEXT NULL,
member_adr_line1 TEXT NULL,
member_adr_line2 TEXT NULL,
member_adr_city TEXT NULL,
member_adr_state TEXT NULL,
member_adr_zip TEXT NULL,
patient_id TEXT NULL,
patient_relation TEXT NULL,
patient_fname TEXT NULL,
patient_lname TEXT NULL,
patient_gender TEXT NULL,
patient_dob TEXT NULL,
patient_age TEXT NULL,
billing_pr_id TEXT NULL,
billing_pr_npi TEXT NULL,
billing_name1 TEXT NULL,
billing_name2 TEXT NULL,
billing_adr_line1 TEXT NULL,
billing_adr_line2 TEXT NULL,
billing_adr_city TEXT NULL,
billing_adr_state TEXT NULL,
billing_adr_zip TEXT NULL,
referring_pr_id TEXT NULL,
referring_pr_npi TEXT NULL,
referring_name1 TEXT NULL,
referring_name2 TEXT NULL,
attending_pr_id TEXT NULL,
attending_pr_npi TEXT NULL,
attending_name1 TEXT NULL,
attending_name2 TEXT NULL,
facility_id TEXT NULL,
facility_name1 TEXT NULL,
facility_name2 TEXT NULL,
facility_adr_line1 TEXT NULL,
facility_adr_line2 TEXT NULL,
facility_adr_city TEXT NULL,
facility_adr_state TEXT NULL,
facility_adr_zip TEXT NULL,
statement_from TEXT NULL,
statement_to TEXT NULL,
total_charge DECIMAL NULL,
total_allowed DECIMAL NULL,
drg_code TEXT NULL,
patient_control TEXT NULL,
type_bill TEXT NULL,
release_sign TEXT NULL,
assignment_sign TEXT NULL,
in_out_network TEXT NULL,
principal_procedure TEXT NULL,
admit_diagnosis TEXT NULL,
other_proc_code_2 TEXT NULL,
other_proc_code_3 TEXT NULL,
other_proc_code_4 TEXT NULL,
other_proc_code_5 TEXT NULL,
other_proc_code_6 TEXT NULL,
prov_specialty TEXT NULL,
type_coverage TEXT NULL,
explanation_code TEXT NULL,
accident_related TEXT NULL,
esrd_patient TEXT NULL,
hosp_admis_or_er TEXT NULL,
amb_nurse_to_hosp TEXT NULL,
not_covrd_specialt TEXT NULL,
electronic_claim TEXT NULL,
dialysis_related TEXT NULL,
new_patient TEXT NULL,
initial_procedure TEXT NULL,
amb_nurse_to_diag TEXT NULL,
amb_hosp_to_hosp TEXT NULL,
admission_date DATE null,
admission_hour TEXT NULL,
admit_type_code TEXT NULL,
admit_src_code TEXT NULL,
discharge_hour TEXT NULL,
patient_status_cd TEXT NULL,
tooth_number TEXT NULL,
other_proc_code_7 TEXT NULL,
other_proc_code_8 TEXT NULL,
other_proc_code_9 TEXT NULL,
other_proc_code_10 TEXT NULL,
billing_taxonomy TEXT NULL,
billing_state_lic TEXT NULL,
billing_upin TEXT NULL,
billing_ssn TEXT NULL,
rendering_taxonomy TEXT NULL,
rendering_state_lic TEXT NULL,
rendering_upin TEXT NULL,
facility_npi TEXT NULL,
facility_state_lic TEXT NULL,
other_proc_code_11 TEXT NULL,
other_proc_code_12 TEXT NULL,
other_proc_code_13 TEXT NULL,
other_proc_code_14 TEXT NULL,
other_proc_code_15 TEXT NULL,
other_proc_code_16 TEXT NULL,
other_proc_code_17 TEXT NULL,
other_proc_code_18 TEXT NULL,
other_proc_code_19 TEXT NULL,
other_proc_code_20 TEXT NULL,
other_proc_code_21 TEXT NULL,
other_proc_code_22 TEXT NULL,
other_proc_code_23 TEXT NULL,
other_proc_code_24 TEXT NULL,
other_proc_code_25 TEXT NULL,
claim_filing_ind_cd TEXT NULL,
referring_pr_state_lic TEXT NULL,
referring_pr_upin TEXT NULL,
referring_pr_commercial TEXT NULL,
pay_to_prov_address_1 TEXT NULL,
pay_to_prov_address_2 TEXT NULL,
pay_to_prov_city TEXT NULL,
pay_to_prov_zip TEXT NULL,
pay_to_prov_state TEXT NULL,
supervising_pr_org_name TEXT NULL,
supervising_pr_last_name TEXT NULL,
supervising_pr_first_name TEXT NULL,
supervising_pr_middle_name TEXT NULL,
supervising_pr_npi TEXT NULL,
supervising_pr_state_lic TEXT NULL,
supervising_pr_upin TEXT NULL,
supervising_pr_commercial TEXT NULL,
supervising_pr_location TEXT NULL,
operating_pr_org_name TEXT NULL,
operating_pr_last_name TEXT NULL,
operating_pr_first_name TEXT NULL,
operating_pr_middle_name TEXT NULL,
operating_pr_npi TEXT NULL,
operating_pr_state_lic TEXT NULL,
operating_pr_upin TEXT NULL,
operating_pr_commercial TEXT NULL,
operating_pr_location TEXT NULL,
oth_operating_pr_org_name TEXT NULL,
oth_operating_pr_last_name TEXT NULL,
oth_operating_pr_first_name TEXT NULL,
oth_operating_pr_middle_name TEXT NULL,
oth_operating_pr_npi TEXT NULL,
oth_operating_pr_state_lic TEXT NULL,
oth_operating_pr_upin TEXT NULL,
oth_operating_pr_commercial TEXT NULL,
oth_operating_pr_location TEXT NULL,
pay_to_plan_name TEXT NULL,
pay_to_plan_address_1 TEXT NULL,
pay_to_plan_address_2 TEXT NULL,
pay_to_plan_city TEXT NULL,
pay_to_plan_zip TEXT NULL,
pay_to_plan_state TEXT NULL,
pay_to_plan_naic_id TEXT NULL,
pay_to_plan_payer_id TEXT NULL,
pay_to_plan_plan_id TEXT NULL,
pay_to_plan_clm_ofc_number TEXT NULL,
pay_to_plan_tax_id TEXT NULL,
cob1_payer_name TEXT NULL,
cob1_payer_id TEXT NULL,
cob1_hpid TEXT NULL,
cob1_resp_seq_cd TEXT NULL,
cob1_relationship_cd TEXT NULL,
cob1_group_policy_nbr TEXT NULL,
cob1_group_name TEXT NULL,
cob1_ins_type_cd TEXT NULL,
cob1_clm_filing_ind_cd TEXT NULL,
cob2_payer_name TEXT NULL,
cob2_payer_id TEXT NULL,
cob2_hpid TEXT NULL,
cob2_resp_seq_cd TEXT NULL,
cob2_relationship_cd TEXT NULL,
cob2_group_policy_nbr TEXT NULL,
cob2_group_name TEXT NULL,
cob2_ins_type_cd TEXT NULL,
cob2_clm_filing_ind_cd TEXT NULL,
line_number INTEGER NULL,
service_from DATE NULL ,
service_to DATE NULL,
place_service TEXT NULL,
procedure TEXT NULL,
procedure_qual TEXT NULL,
procedure_modifier_1 TEXT NULL,
procedure_modifier_2 TEXT NULL,
procedure_modifier_3 TEXT NULL,
procedure_modifier_4 TEXT NULL,
line_charge DECIMAL NULL,
line_allowed DECIMAL NULL,
units TEXT NULL,
revenue_code TEXT NULL,
ndc TEXT NULL,
ambulance_to_hosp TEXT NULL,
emergency TEXT NULL,
tooth_surface TEXT NULL,
oral_cavity TEXT NULL,
type_service TEXT NULL,
copay TEXT NULL,
paid_amount TEXT NULL,
date_paid TEXT NULL,
bene_not_entitled TEXT NULL,
patient_reach_max TEXT NULL,
svc_during_postop TEXT NULL,
adjudicated_procedure TEXT NULL,
adjudicated_procedure_qual TEXT NULL,
adjudicated_proc_modifier_1 TEXT NULL,
adjudicated_proc_modifier_2 TEXT NULL,
adjudicated_proc_modifier_3 TEXT NULL,
adjudicated_proc_modifier_4 TEXT NULL,
diagnosis text NULL
);
