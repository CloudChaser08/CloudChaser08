SELECT
    claim_number,
    payer_id,
    coding_type,
    received_date,
    claim_type_code,
    payer_name,
    member_adr_state,
    member_adr_zip,
    patient_gender,
    patient_yob,
    patient_age,
    billing_pr_id,
    billing_pr_npi,
    billing_name1,
    billing_name2,
    billing_adr_line1,
    billing_adr_line2,
    billing_adr_city,
    billing_adr_state,
    billing_adr_zip,
    referring_pr_npi,
    referring_name1,
    referring_name2,
    attending_pr_npi,
    attending_name1,
    attending_name2,
    facility_name1,
    facility_name2,
    facility_adr_line1,
    facility_adr_line2,
    facility_adr_city,
    facility_adr_state,
    facility_adr_zip,
    statement_from,
    statement_to,
    total_charge,
    total_allowed,
    drg_code,
    type_bill,
    principal_procedure,
    admit_diagnosis,
    primary_diagnosis,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    other_proc_code_2,
    other_proc_code_3,
    other_proc_code_4,
    other_proc_code_5,
    other_proc_code_6,
    prov_specialty,
    type_coverage,
    initial_procedure,
    amb_nurse_to_diag,
    admission_date,
    admit_type_code,
    admit_src_code,
    patient_status_cd,
    other_proc_code_7,
    other_proc_code_8,
    other_proc_code_9,
    other_proc_code_10,
    billing_taxonomy,
    billing_state_lic,
    billing_upin,
    billing_ssn,
    rendering_taxonomy,
    rendering_state_lic,
    rendering_upin,
    facility_npi,
    facility_state_lic,
    hvjoinkey,
    data_set
FROM waystar_claims_hist_dedup clm
INNER JOIN
(
    SELECT
        claim_number       AS clm_num,
        MAX(received_date) AS rec_dt
     FROM waystar_claims_hist_dedup
    GROUP BY 1
) sub
  ON clm.claim_number = sub.clm_num
  AND COALESCE(clm.received_date, '19000101') = COALESCE(sub.rec_dt, '19000101')
