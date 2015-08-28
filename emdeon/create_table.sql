
CREATE TABLE emdeon_claim (
    claim_number char(18) distkey,
    record_type text,
    payer_id text,
    coding_type text,
    received_date date,
    claim_type_code text,
    contract_allow_ind text,
    payer_name text,
    sub_client_id text,
    group_name text,
    member_id text,
    member_fname text,
    member_lname text,
    member_gender text,
    member_dob text,
    member_adr_line1 text,
    member_adr_line2 text,
    member_adr_city text,
    member_adr_state text,
    member_adr_zip text,
    patient_id text,
    patient_relation text,
    patient_fname text,
    patient_lname text,
    patient_gender text,
    patient_dob text,
    patient_age text,
    billing_pr_id text,
    billing_pr_npi text,
    billing_name1 text,
    billing_name2 text,
    billing_adr_line1 text,
    billing_adr_line2 text,
    billing_adr_city text,
    billing_adr_state text,
    billing_adr_zip text,
    referring_pr_id text,
    referring_pr_npi text,
    referring_name1 text,
    referring_name2 text,
    attending_pr_id text,
    attending_pr_npi text,
    attending_name1 text,
    attending_name2 text,
    facility_id text,
    facility_name1 text,
    facility_name2 text,
    facility_adr_line1 text,
    facility_adr_line2 text,
    facility_adr_city text,
    facility_adr_state text,
    facility_adr_zip text,
    statement_from text,
    statement_to text,
    total_charge text,
    total_allowed text,
    drg_code text,
    patient_control text,
    type_bill text,
    release_sign text,
    assignment_sign text,
    in_out_network text,
    principal_procedure text,
    admit_diagnosis text,
    primary_diagnosis text,
    diagnosis_code_2 text,
    diagnosis_code_3 text,
    diagnosis_code_4 text,
    diagnosis_code_5 text,
    diagnosis_code_6 text,
    diagnosis_code_7 text,
    diagnosis_code_8 text,
    other_proc_code_2 text,
    other_proc_code_3 text,
    other_proc_code_4 text,
    other_proc_code_5 text,
    other_proc_code_6 text,
    prov_specialty text,
    type_coverage text,
    explanation_code text,
    accident_related text,
    esrd_patient text,
    hosp_admis_or_er text,
    amb_nurse_to_hosp text,
    not_covrd_specialt text,
    electronic_claim text,
    dialysis_related text,
    new_patient text,
    initial_procedure text,
    amb_nurse_to_diag text,
    amb_hosp_to_hosp text,
    admission_date text,
    admission_hour text,
    admit_type_code text,
    admit_src_code text,
    discharge_hour text,
    patient_status_cd text,
    tooth_number text,
    other_proc_code_7 text,
    other_proc_code_8 text,
    other_proc_code_9 text,
    other_proc_code_10 text,
    billing_taxonomy text,
    billing_state_lic text,
    billing_upin text,
    billing_ssn text,
    rendering_taxonomy text,
    rendering_state_lic text,
    rendering_upin text,
    facility_npi text,
    facility_state_lic text
)
SORTKEY(received_date);

CREATE TABLE emdeon_diag (
  claim_number char(18) distkey,
   record_type text,
   diagnosis_code_9 text,
   diagnosis_code_10 text,
   diagnosis_code_11 text,
   diagnosis_code_12 text,
   diagnosis_code_13 text,
   diagnosis_code_14 text,
   diagnosis_code_15 text,
   diagnosis_code_16 text,
   diagnosis_code_17 text,
   diagnosis_code_18 text,
   diagnosis_code_19 text,
   diagnosis_code_20 text,
   diagnosis_code_21 text,
   diagnosis_code_22 text,
   diagnosis_code_23 text,
   diagnosis_code_24 text,
   diagnosis_code_25 text,
   other_proc_code_11 text,
   other_proc_code_12 text,
   other_proc_code_13 text,
   other_proc_code_14 text,
   other_proc_code_15 text,
   other_proc_code_16 text,
   other_proc_code_17 text,
   other_proc_code_18 text,
   other_proc_code_19 text,
   other_proc_code_20 text,
   other_proc_code_21 text,
   other_proc_code_22 text,
   other_proc_code_23 text,
   other_proc_code_24 text,
   other_proc_code_25 text,
   claim_filing_ind_cd text,
   referring_pr_state_lic text,
   referring_pr_upin text,
   referring_pr_commercial text,
   pay_to_prov_address_1 text,
   pay_to_prov_address_2 text,
   pay_to_prov_city text,
   pay_to_prov_zip text,
   pay_to_prov_state text,
   supervising_pr_org_name text,
   supervising_pr_last_name text,
   supervising_pr_first_name text,
   supervising_pr_middle_name text,
   supervising_pr_npi text,
   supervising_pr_state_lic text,
   supervising_pr_upin text,
   supervising_pr_commercial text,
   supervising_pr_location text,
   operating_pr_org_name text,
   operating_pr_last_name text,
   operating_pr_first_name text,
   operating_pr_middle_name text,
   operating_pr_npi text,
   operating_pr_state_lic text,
   operating_pr_upin text,
   operating_pr_commercial text,
   operating_pr_location text,
   oth_operating_pr_org_name text,
   oth_operating_pr_last_name text,
   oth_operating_pr_first_name text,
   oth_operating_pr_middle_name text,
   oth_operating_pr_npi text,
   oth_operating_pr_state_lic text,
   oth_operating_pr_upin text,
   oth_operating_pr_commercial text,
   oth_operating_pr_location text,
   pay_to_plan_name text,
   pay_to_plan_address_1 text,
   pay_to_plan_address_2 text,
   pay_to_plan_city text,
   pay_to_plan_zip text,
   pay_to_plan_state text,
   pay_to_plan_naic_id text,
   pay_to_plan_payer_id text,
   pay_to_plan_plan_id text,
   pay_to_plan_clm_ofc_number text,
   pay_to_plan_tax_id text,
   cob1_payer_name text,
   cob1_payer_id text,
   cob1_hpid text,
   cob1_resp_seq_cd text,
   cob1_relationship_cd text,
   cob1_group_policy_nbr text,
   cob1_group_name text,
   cob1_ins_type_cd text,
   cob1_clm_filing_ind_cd text,
   cob2_payer_name text,
   cob2_payer_id text,
   cob2_hpid text,
   cob2_resp_seq_cd text,
   cob2_relationship_cd text,
   cob2_group_policy_nbr text,
   cob2_group_name text,
   cob2_ins_type_cd text,
   cob2_clm_filing_ind_cd text,
   cob3_payer_name text,
   cob3_payer_id text,
   cob3_hpid text,
   cob3_resp_seq_cd text,
   cob3_relationship_cd text,
   cob3_group_policy_nbr text,
   cob3_group_name text,
   cob3_ins_type_cd text,
   cob3_clm_filing_ind_cd text,
   cob4_payer_name text,
   cob4_payer_id text,
   cob4_hpid text,
   cob4_resp_seq_cd text,
   cob4_relationship_cd text,
   cob4_group_policy_nbr text,
   cob4_group_name text,
   cob4_ins_type_cd text,
   cob4_clm_filing_ind_cd text,
   cob5_payer_name text,
   cob5_payer_id text,
   cob5_hpid text,
   cob5_resp_seq_cd text,
   cob5_relationship_cd text,
   cob5_group_policy_nbr text,
   cob5_group_name text,
   cob5_ins_type_cd text,
   cob5_clm_filing_ind_cd text,
   cob6_payer_name text,
   cob6_payer_id text,
   cob6_hpid text,
   cob6_resp_seq_cd text,
   cob6_relationship_cd text,
   cob6_group_policy_nbr text,
   cob6_group_name text,
   cob6_ins_type_cd text,
   cob6_clm_filing_ind_cd text,
   cob7_payer_name text,
   cob7_payer_id text,
   cob7_hpid text,
   cob7_resp_seq_cd text,
   cob7_relationship_cd text,
   cob7_group_policy_nbr text,
   cob7_group_name text,
   cob7_ins_type_cd text,
   cob7_clm_filing_ind_cd text,
   cob8_payer_name text,
   cob8_payer_id text,
   cob8_hpid text,
   cob8_resp_seq_cd text,
   cob8_relationship_cd text,
   cob8_group_policy_nbr text,
   cob8_group_name text,
   cob8_ins_type_cd text,
   cob8_clm_filing_ind_cd text,
   cob9_payer_name text,
   cob9_payer_id text,
   cob9_hpid text,
   cob9_resp_seq_cd text,
   cob9_relationship_cd text,
   cob9_group_policy_nbr text,
   cob9_group_name text,
   cob9_ins_type_cd text,
   cob9_clm_filing_ind_cd text,
   cob10_payer_name text,
   cob10_payer_id text,
   cob10_hpid text,
   cob10_resp_seq_cd text,
   cob10_relationship_cd text,
   cob10_group_policy_nbr text,
   cob10_group_name text,
   cob10_ins_type_cd text,
   cob10_clm_filing_ind_cd text
);

 CREATE TABLE emdeon_service (
     claim_number char(18) distkey,
     record_type text,
     line_number text,
     service_from date,
     service_to date,
     place_service text,
     procedure text,
     procedure_qual text,
     procedure_modifier_1 text,
     procedure_modifier_2 text,
     procedure_modifier_3 text,
     procedure_modifier_4 text,
     line_charge text,
     line_allowed text,
     units text,
     revenue_code text,
     diagnosis_pointer_1 text,
     diagnosis_pointer_2 text,
     diagnosis_pointer_3 text,
     diagnosis_pointer_4 text,
     diagnosis_pointer_5 text,
     diagnosis_pointer_6 text,
     diagnosis_pointer_7 text,
     diagnosis_pointer_8 text,
     ndc text,
     ambulance_to_hosp text,
     emergency text,
     tooth_surface text,
     oral_cavity text,
     type_service text,
     copay text,
     paid_amount text,
     date_paid text,
     bene_not_entitled text,
     patient_reach_max text,
     svc_during_postop text,
     adjudicated_procedure text,
     adjudicated_procedure_qual text,
     adjudicated_proc_modifier_1 text,
     adjudicated_proc_modifier_2 text,
     adjudicated_proc_modifier_3 text,
     adjudicated_proc_modifier_4 text
)
SORTKEY(service_from, service_to);
