DROP TABLE IF EXISTS emdeon_institutional_claims_extended;
SELECT * INTO emdeon_institutional_claims_extended
FROM emdeon_dx_raw_claims LEFT JOIN emdeon_dx_raw_diagnosis USING (claim_id)
WHERE claim_type = 'I';

DROP TABLE IF EXISTS emdeon_institutional_claims_all;
SELECT * INTO emdeon_institutional_claims_all
FROM emdeon_institutional_claims_extended 
    INNER JOIN emdeon_dx_raw_service USING (claim_id);

INSERT INTO medicalclaims_common_model (claim_id,
hvid,
patient_gender,
patient_age,
patient_year_of_birth,
patient_zip3,
patient_state,
claim_type,
date_received,
date_service,
date_service_end,
inst_date_admitted,
inst_admit_type_std_id,
inst_admit_source_std_id,
inst_discharge_status_std_id,
inst_type_of_bill_std_id,
inst_drg_std_id,
place_of_service_std_id,
service_line_number,
diagnosis_code_qual,
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
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_vendor_id,
payer_name,
payer_parent_name,
prov_rendering_tax_id,
prov_rendering_state_license,
prov_rendering_upin,
prov_rendering_name_1,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_rendering_vendor_specialty,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_state_license,
prov_billing_upin,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_state_license,
prov_referring_upin,
prov_referring_commercial_id,
prov_referring_name_1,
prov_referring_name_2,
prov_facility_tax_id,
prov_facility_state_license,
prov_facility_name_1,
prov_facility_name_2,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2)
SELECT b.claim_id,
hvid,
patient_gender,
patient_age,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN date_service IS NOT NULL AND (patient_dob >= (extract('year' from date_service::date - '32873 days'::interval)::text)) AND patient_dob <= (extract('year' from getdate())::text) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threeDigitZip as patient_zip3,
state as patient_state,
claim_type,
date_received,
CASE WHEN date_service IS NOT NULL THEN date_service ELSE statement_from END,
CASE WHEN date_service_end IS NOT NULL THEN date_service ELSE statement_to END,
inst_date_admitted,
inst_admit_type_std_id,
inst_admit_source_std_id,
inst_discharge_status_std_id,
inst_type_of_bill_std_id,
inst_drg_std_id,
CASE WHEN place_of_service_std_id IS NOT NULL THEN place_of_service_std_id ELSE substring(inst_type_of_bill_std_id , 1, 2) END,
service_line_number,
CASE WHEN diagnosis_code_qual = '9' THEN '01' WHEN diagnosis_code_qual = 'X' THEN '02' END,
upper(replace(replace(procedure_code, ' ', ''), ',', '')) AS procedure_code,
procedure_code_qual,
CASE WHEN LEN(UPPER(BTRIM(procedure_code))) AND UPPER(BTRIM(procedure_code)) = UPPER(BTRIM(principal_procedure)) THEN 1 ELSE NULL END AS principal_proc_ind,
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
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_mapping.payer_vendor_id,
payer_mapping.payer_name,
payer_mapping.payer_parent_name,
prov_rendering_tax_id,
prov_rendering_state_license,
prov_rendering_upin,
prov_rendering_name_1,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_specialty AS prov_rendering_vendor_specialty,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_state_license,
prov_billing_upin,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_state_license,
prov_referring_upin,
prov_referring_commercial_id,
prov_referring_name_1,
prov_referring_name_2,
prov_facility_tax_id,
prov_facility_state_license,
prov_facility_name_1,
prov_facility_name_2,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_id_1 AS cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_id_2 AS cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2
FROM emdeon_institutional_claims_all b
    LEFT JOIN matching_payload ON b.claim_id = claimId
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
    LEFT JOIN payer_mapping USING (payer_vendor_id);

DROP TABLE IF EXISTS service_loc;
SELECT DISTINCT claim_id, place_of_service_std_id INTO service_loc FROM emdeon_dx_raw_service;

DROP TABLE IF EXISTS emdeon_institutional_claims_unrelated;
SELECT *, uniquify((COALESCE(primary_diagnosis, '')  || ':' || COALESCE(diagnosis_code_2, '')  || ':' || COALESCE(diagnosis_code_3, '')  || ':' || COALESCE(diagnosis_code_4, '')  || ':' || COALESCE(diagnosis_code_5, '')  || ':' || COALESCE(diagnosis_code_6, '')  || ':' || COALESCE(diagnosis_code_7, '')  || ':' || COALESCE(diagnosis_code_8, '')  || ':' || COALESCE(diagnosis_code_9, '')  || ':' || COALESCE(diagnosis_code_10, '')  || ':' || COALESCE(diagnosis_code_11, '')  || ':' || COALESCE(diagnosis_code_12, '')  || ':' || COALESCE(diagnosis_code_13, '')  || ':' || COALESCE(diagnosis_code_14, '')  || ':' || COALESCE(diagnosis_code_15, '')  || ':' || COALESCE(diagnosis_code_16, '')  || ':' || COALESCE(diagnosis_code_17, '')  || ':' || COALESCE(diagnosis_code_18, '')  || ':' || COALESCE(diagnosis_code_19, '')  || ':' || COALESCE(diagnosis_code_20, '')  || ':' || COALESCE(diagnosis_code_21, '')  || ':' || COALESCE(diagnosis_code_22, '')  || ':' || COALESCE(diagnosis_code_23, '')  || ':' || COALESCE(diagnosis_code_24, '')  || ':' || COALESCE(diagnosis_code_25, '')  || ':' || COALESCE(admit_diagnosis, ''))) as diag_concat INTO emdeon_institutional_claims_unrelated FROM emdeon_institutional_claims_extended LEFT JOIN service_loc USING (claim_id);


INSERT INTO medicalclaims_common_model (claim_id,
hvid,
patient_gender,
patient_age,
patient_year_of_birth,
patient_zip3,
patient_state,
claim_type,
date_received,
date_service,
date_service_end,
inst_date_admitted,
inst_admit_type_std_id,
inst_admit_source_std_id,
inst_discharge_status_std_id,
inst_type_of_bill_std_id,
inst_drg_std_id,
place_of_service_std_id,
diagnosis_code,
diagnosis_code_qual,
admit_diagnosis_ind,
medical_coverage_type,
total_charge,
total_allowed,
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_vendor_id,
payer_name,
payer_parent_name,
prov_rendering_tax_id,
prov_rendering_state_license,
prov_rendering_upin,
prov_rendering_name_1,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_rendering_vendor_specialty,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_state_license,
prov_billing_upin,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_state_license,
prov_referring_upin,
prov_referring_commercial_id,
prov_referring_name_1,
prov_referring_name_2,
prov_facility_tax_id,
prov_facility_state_license,
prov_facility_name_1,
prov_facility_name_2,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2)
SELECT b.claim_id,
hvid,
patient_gender,
patient_age,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN statement_from IS NOT NULL AND (patient_dob >= (extract('year' from statement_from::date - '32873 days'::interval)::text)) AND patient_dob <= (extract('year' from getdate())::text) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threeDigitZip as patient_zip3,
state as patient_state,
claim_type,
date_received,
statement_from,
statement_to,
inst_date_admitted,
inst_admit_type_std_id,
inst_admit_source_std_id,
inst_discharge_status_std_id,
inst_type_of_bill_std_id,
inst_drg_std_id,
CASE WHEN place_of_service_std_id IS NOT NULL THEN place_of_service_std_id ELSE substring(inst_type_of_bill_std_id , 1, 2) END,
upper(replace(replace(replace(split_part(diag_concat,':',n), ' ', ''), ',', ''), '.', '')) AS diagnosis_code,
CASE WHEN diagnosis_code_qual = '9' THEN '01' WHEN diagnosis_code_qual = 'X' THEN '02' END,
CASE WHEN LEN(UPPER(BTRIM(split_part(diag_concat,':',n)))) AND UPPER(BTRIM(split_part(diag_concat,':',n))) = UPPER(BTRIM(admit_diagnosis)) THEN 1 ELSE NULL END AS admit_diagnosis_ind,
medical_coverage_type,
total_charge,
total_allowed,
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_mapping.payer_vendor_id,
payer_mapping.payer_name,
payer_mapping.payer_parent_name,
prov_rendering_tax_id,
prov_rendering_state_license,
prov_rendering_upin,
prov_rendering_name_1,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_specialty AS prov_rendering_vendor_specialty,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_state_license,
prov_billing_upin,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_state_license,
prov_referring_upin,
prov_referring_commercial_id,
prov_referring_name_1,
prov_referring_name_2,
prov_facility_tax_id,
prov_facility_state_license,
prov_facility_name_1,
prov_facility_name_2,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_id_1 AS cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_id_2 AS cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2
FROM emdeon_institutional_claims_unrelated b
    CROSS JOIN split_indices
    LEFT JOIN matching_payload ON b.claim_id = claimid
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
    LEFT JOIN payer_mapping USING (payer_vendor_id)
WHERE split_part(diag_concat,':',n) IS NOT NULL AND split_part(diag_concat,':',n) != '';

DROP TABLE IF EXISTS together;        
CREATE TABLE together (claim_id text ENCODE lzo, unrelated varchar(5000) ENCODE lzo, principal_procedure_check text ENCODE lzo);

DROP TABLE IF EXISTS emdeon_institutional_claims_procedures;
SELECT DISTINCT claim_id, procedure_code INTO emdeon_institutional_claims_procedures FROM emdeon_institutional_claims_all;

INSERT INTO together
SELECT a.claim_id, string_set_diff(unrelated_concat, related_concat) AS unrelated, principal_procedure_check
FROM
(SELECT claim_id, listagg(procedure_code, ':') within group (order by procedure_code) AS related_concat FROM emdeon_institutional_claims_procedures group by claim_id) a
INNER JOIN
(SELECT claim_id, principal_procedure as principal_procedure_check, (
COALESCE(principal_procedure, '') || ':' || COALESCE(other_proc_code_2, '') || ':' || COALESCE(other_proc_code_3, '') || ':' || COALESCE(other_proc_code_4, '') || ':' || COALESCE(other_proc_code_5, '') || ':' || COALESCE(other_proc_code_6, '') || ':' || COALESCE(other_proc_code_7, '') || ':' || COALESCE(other_proc_code_8, '') || ':' || COALESCE(other_proc_code_9, '') || ':' || COALESCE(other_proc_code_10, '') || ':' || COALESCE(other_proc_code_11, '') || ':' || COALESCE(other_proc_code_12, '') || ':' || COALESCE(other_proc_code_13, '') || ':' || COALESCE(other_proc_code_14, '') || ':' || COALESCE(other_proc_code_15, '') || ':' || COALESCE(other_proc_code_16, '') || ':' || COALESCE(other_proc_code_17, '') || ':' || COALESCE(other_proc_code_18, '') || ':' || COALESCE(other_proc_code_19, '') || ':' || COALESCE(other_proc_code_20, '') || ':' || COALESCE(other_proc_code_21, '') || ':' || COALESCE(other_proc_code_22, '') || ':' || COALESCE(other_proc_code_23, '') || ':' || COALESCE(other_proc_code_24, '') || ':' || COALESCE(other_proc_code_25, '') ) AS unrelated_concat
FROM 
emdeon_institutional_claims_unrelated) b USING (claim_id);

DROP TABLE emdeon_institutional_claims_unrelated_procs;
SELECT * INTO emdeon_institutional_claims_unrelated_procs
FROM together INNER JOIN emdeon_institutional_claims_unrelated USING (claim_id);

INSERT INTO medicalclaims_common_model (claim_id,
hvid,
patient_gender,
patient_age,
patient_year_of_birth,
patient_zip3,
patient_state,
claim_type,
date_received,
date_service,
date_service_end,
inst_date_admitted,
inst_admit_type_std_id,
inst_admit_source_std_id,
inst_discharge_status_std_id,
inst_type_of_bill_std_id,
inst_drg_std_id,
diagnosis_code_qual,
procedure_code,
principal_proc_ind,
medical_coverage_type,
total_charge,
total_allowed,
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_vendor_id,
payer_name,
payer_parent_name,
prov_rendering_tax_id,
prov_rendering_state_license,
prov_rendering_upin,
prov_rendering_name_1,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_rendering_vendor_specialty,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_state_license,
prov_billing_upin,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_state_license,
prov_referring_upin,
prov_referring_commercial_id,
prov_referring_name_1,
prov_referring_name_2,
prov_facility_tax_id,
prov_facility_state_license,
prov_facility_name_1,
prov_facility_name_2,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2)
SELECT b.claim_id,
hvid,
patient_gender,
patient_age,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN inst_date_admitted IS NOT NULL AND (patient_dob >= (extract('year' from inst_date_admitted::date - '32873 days'::interval)::text)) AND patient_dob <= (extract('year' from getdate())::text) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threeDigitZip as patient_zip3,
state as patient_state,
claim_type,
date_received,
statement_from,
statement_to,
inst_date_admitted,
inst_admit_type_std_id,
inst_admit_source_std_id,
inst_discharge_status_std_id,
inst_type_of_bill_std_id,
inst_drg_std_id,
CASE WHEN diagnosis_code_qual = '9' THEN '01' WHEN diagnosis_code_qual = 'X' THEN '02' END,
upper(replace(replace(split_part(unrelated,':',n), ' ', ''), ',', '')) AS procedure_code,
CASE WHEN LEN(UPPER(BTRIM(split_part(unrelated,':',n)))) AND UPPER(BTRIM(split_part(unrelated,':',n))) = UPPER(BTRIM(principal_procedure)) THEN 1 ELSE NULL END AS principal_proc_ind,
medical_coverage_type,
total_charge,
total_allowed,
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_mapping.payer_vendor_id,
payer_mapping.payer_name,
payer_mapping.payer_parent_name,
prov_rendering_tax_id,
prov_rendering_state_license,
prov_rendering_upin,
prov_rendering_name_1,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_specialty AS prov_rendering_vendor_specialty,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_state_license,
prov_billing_upin,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_state_license,
prov_referring_upin,
prov_referring_commercial_id,
prov_referring_name_1,
prov_referring_name_2,
prov_facility_tax_id,
prov_facility_state_license,
prov_facility_name_1,
prov_facility_name_2,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_id_1 AS cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_hpid_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_id_2 AS cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_hpid_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2
FROM emdeon_institutional_claims_unrelated_procs b
    CROSS JOIN split_indices
    LEFT JOIN matching_payload ON b.claim_id = claimid
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
    LEFT JOIN payer_mapping USING (payer_vendor_id)
WHERE split_part(unrelated,':',n) IS NOT NULL AND split_part(unrelated,':',n) != '';
