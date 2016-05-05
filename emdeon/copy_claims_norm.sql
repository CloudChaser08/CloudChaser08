COPY emdeon_claim_norm
(claim_id, payer_vendor_id, diagnosis_code_qual, date_received, claim_type, contract_allow_ind, payer_name, sub_client_id, group_name, member_id,
member_fname, member_lname, member_gender, member_dob, member_adr_line1, member_adr_line2, member_adr_city, member_adr_state, member_adr_zip,
patient_id, patient_relation, patient_fname, patient_lname, patient_gender, patient_dob, patient_age, prov_billing_tax_id, prov_billing_npi, prov_billing_name_1,
prov_billing_name_2, prov_billing_address_1, prov_billing_address_2, prov_billing_city, prov_billing_state, prov_billing_zip, prov_referring_tax_id, prov_referring_npi, prov_referring_name_1,
prov_referring_name_2, prov_rendering_tax_id, prov_rendering_npi, prov_rendering_name_1, prov_rendering_name_2, prov_facility_tax_id, prov_facility_name_1, prov_facility_name_2,
prov_facility_address_1, prov_facility_address_2, prov_facility_city, prov_facility_state, prov_facility_zip, statement_from, statement_to, total_charge, total_allowed, inst_drg_std_id,
patient_control, inst_type_of_bill_std_id, release_sign, assignment_sign, in_out_network, prov_specialty, medical_coverage_type, explanation_code, accident_related, esrd_patient, hosp_admis_or_er,
amb_nurse_to_hosp, not_covrd_specialt, electronic_claim, dialysis_related, new_patient, initial_procedure, amb_nurse_to_diag, amb_hosp_to_hosp, inst_date_admitted, admission_hour, inst_admit_type_std_id, inst_admit_source_std_id,
discharge_hour, inst_discharge_status_std_id, tooth_number, prov_billing_std_taxonomy, prov_billing_state_license, prov_billing_upin, prov_billing_ssn, prov_rendering_std_taxonomy, prov_rendering_state_license, prov_rendering_upin,
prov_facility_npi, prov_facility_state_license, claim_filing_ind_cd, prov_referring_state_license, prov_referring_upin, prov_referring_commercial_id, pay_to_prov_address_1, pay_to_prov_address_2, pay_to_prov_city,
pay_to_prov_zip, pay_to_prov_state, supervising_pr_org_name, supervising_pr_last_name, supervising_pr_first_name, supervising_pr_middle_name, supervising_pr_npi, supervising_pr_state_lic, supervising_pr_upin,
supervising_pr_commercial, supervising_pr_location, operating_pr_org_name, operating_pr_last_name, operating_pr_first_name, operating_pr_middle_name, operating_pr_npi, operating_pr_state_lic,
operating_pr_upin, operating_pr_commercial, operating_pr_location, oth_operating_pr_org_name, oth_operating_pr_last_name, oth_operating_pr_first_name, oth_operating_pr_middle_name, oth_operating_pr_npi, oth_operating_pr_state_lic,
oth_operating_pr_upin, oth_operating_pr_commercial, oth_operating_pr_location, pay_to_plan_name, pay_to_plan_address_1, pay_to_plan_address_2, pay_to_plan_city, pay_to_plan_zip,
pay_to_plan_state, pay_to_plan_naic_id, pay_to_plan_payer_id, pay_to_plan_plan_id, pay_to_plan_clm_ofc_number, pay_to_plan_tax_id, cob_payer_name_1, cob_payer_id_1, cob_payer_hpid_1,
cob_payer_seq_code_1, cob_relationship_cd_1, cob_group_policy_nbr_1, cob_group_name_1, cob_ins_type_code_1, cob_payer_claim_filing_ind_code_1, cob_payer_name_2, cob_payer_id_2, cob_payer_hpid_2,
cob_payer_seq_code_2, cob_relationship_cd_2, cob_group_policy_nbr_2, cob_group_name_2, cob_ins_type_code_2, cob_payer_claim_filing_ind_code_2, service_line_number, date_service, date_service_end,
place_of_service_std_id, procedure_code, procedure_code_qual, procedure_modifier_1, procedure_modifier_2, procedure_modifier_3, procedure_modifier_4, line_charge, line_allowed, procedure_units,
revenue_code, ndc_code, ambulance_to_hosp, emergency, tooth_surface, oral_cavity, type_service, copay, paid_amount, date_paid,
bene_not_entitled, patient_reach_max, svc_during_postop, adjudicated_procedure, adjudicated_procedure_qual, adjudicated_proc_modifier_1, adjudicated_proc_modifier_2,
adjudicated_proc_modifier_3, adjudicated_proc_modifier_4, principal_proc_ind, diagnosis_code, admit_diagnosis_ind, diagnosis_priority,hvid)

FROM 's3://salusv/provider/linked/marketplace/v1/medical/normalized_csv/201501/2016-05-05:14:03:39/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
ACCEPTANYDATE
BLANKSASNULL
EMPTYASNULL
MAXERROR 100000
COMPUPDATE ON
IGNOREHEADER 1
NULL AS 'null'
DELIMITER '|' bzip2
