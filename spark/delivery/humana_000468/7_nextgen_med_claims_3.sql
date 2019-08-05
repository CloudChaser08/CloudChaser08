SELECT 
    prc.row_id                                                                              AS record_id,
    prc.hv_proc_id                                                                          AS claim_id,
    prc.hvid                                                                                AS hvid,
    prc.crt_dt                                                                              AS created,
    '7'                                                                                     AS model_version,
    prc.data_set_nm                                                                         AS data_set,
    CAST(prc.hvm_vdr_feed_id AS STRING)                                                     AS data_feed,
    CAST(prc.hvm_vdr_id AS STRING)                                                          AS data_vendor,
    prc.src_vrsn_id                                                                         AS source_version,
    prc.vdr_org_id                                                                          AS vendor_org_id,
    prc.ptnt_gender_cd                                                                      AS patient_gender,
    prc.ptnt_age_num                                                                        AS patient_age,
    CAST(prc.ptnt_birth_yr AS STRING)                                                       AS patient_year_of_birth,
    prc.ptnt_zip3_cd                                                                        AS patient_zip3,
    prc.ptnt_state_cd                                                                       AS patient_state,
    CAST(NULL AS STRING)                                                                    AS claim_type,
    CAST(NULL AS DATE)                                                                      AS date_received,
    prc.hv_proc_dt                                                                          AS date_service, 
    CAST(NULL AS DATE)                                                                      AS date_service_end,
    CAST(NULL AS DATE)                                                                      AS inst_date_admitted,
    CAST(NULL AS DATE)                                                                      AS inst_date_discharged,
    CAST(NULL AS STRING)                                                                    AS inst_admit_type_std_id,
    CAST(NULL AS STRING)                                                                    AS inst_admit_type_vendor_id,
    CAST(NULL AS STRING)                                                                    AS inst_admit_type_vendor_desc,
    CAST(NULL AS STRING)                                                                    AS inst_admit_source_std_id,
    CAST(NULL AS STRING)                                                                    AS inst_admit_source_vendor_id,
    CAST(NULL AS STRING)                                                                    AS inst_admit_source_vendor_desc,
    CAST(NULL AS STRING)                                                                    AS inst_discharge_status_std_id,
    CAST(NULL AS STRING)                                                                    AS inst_discharge_status_vendor_id,
    CAST(NULL AS STRING)                                                                    AS inst_discharge_status_vendor_desc,
    CAST(NULL AS STRING)                                                                    AS inst_type_of_bill_std_id,
    CAST(NULL AS STRING)                                                                    AS inst_type_of_bill_vendor_id,
    CAST(NULL AS STRING)                                                                    AS inst_type_of_bill_vendor_desc,
    CAST(NULL AS STRING)                                                                    AS inst_drg_std_id,
    CAST(NULL AS STRING)                                                                    AS inst_drg_vendor_id,
    CAST(NULL AS STRING)                                                                    AS inst_drg_vendor_desc,
    CAST(NULL AS STRING)                                                                    AS place_of_service_std_id,
    CAST(NULL AS STRING)                                                                    AS place_of_service_vendor_id,
    CAST(NULL AS STRING)                                                                    AS place_of_service_vendor_desc,
    CAST(NULL AS STRING)                                                                    AS service_line_number,
    prc.proc_diag_cd                                                                        AS diagnosis_code,
    prc.proc_diag_cd_qual                                                                   AS diagnosis_code_qual,
    CAST(NULL AS STRING)                                                                    AS diagnosis_priority,
    CAST(NULL AS STRING)                                                                    AS admit_diagnosis_ind,
    prc.proc_cd                                                                             AS procedure_code,
    /* procedure_code_qual */
    CASE 
        WHEN prc.proc_cd_qual IN ('CPT', 'CPTCODE', 'HCPCS') THEN 'HC'
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS procedure_code_qual,
    CAST(NULL AS STRING)                                                                    AS principal_proc_ind,
    CAST(NULL AS FLOAT)                                                                     AS procedure_units_billed,
    CAST(NULL AS FLOAT)                                                                     AS procedure_units_paid,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_1,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_2,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_3,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_4,
    CAST(NULL AS STRING)                                                                    AS revenue_code,
    CAST(NULL AS STRING)                                                                    AS ndc_code,
    CAST(NULL AS STRING)                                                                    AS medical_coverage_type,
    CAST(NULL AS FLOAT)                                                                     AS line_charge,
    CAST(NULL AS FLOAT)                                                                     AS line_allowed,
    CAST(NULL AS FLOAT)                                                                     AS total_charge,
    CAST(NULL AS FLOAT)                                                                     AS total_allowed,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_npi,
    CAST(NULL AS STRING)                                                                    AS prov_billing_npi,
    CAST(NULL AS STRING)                                                                    AS prov_referring_npi,
    CAST(NULL AS STRING)                                                                    AS prov_facility_npi,
    CAST(NULL AS STRING)                                                                    AS payer_vendor_id,
    CAST(NULL AS STRING)                                                                    AS payer_name,
    CAST(NULL AS STRING)                                                                    AS payer_parent_name,
    CAST(NULL AS STRING)                                                                    AS payer_org_name,
    CAST(NULL AS STRING)                                                                    AS payer_plan_id,
    CAST(NULL AS STRING)                                                                    AS payer_plan_name,
    CAST(NULL AS STRING)                                                                    AS payer_type,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_tax_id,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_dea_id,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_ssn,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_state_license,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_upin,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_commercial_id,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_name_2,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_address_1,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_address_2,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_city,
    /* prov_rendering_state */
    CASE
        WHEN prc.proc_prov_state_cd_qual = 'RENDERING_PROVIDER'
            THEN prc.proc_prov_state_cd
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_rendering_state, 
    /* prov_rendering_zip */
    CASE
        WHEN prc.proc_prov_zip_cd_qual = 'RENDERING_PROVIDER'
            THEN prc.proc_prov_zip_cd
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_rendering_zip,
    /* prov_rendering_std_taxonomy */
    CASE
        WHEN prc.proc_prov_taxnmy_id_qual = 'RENDERING_PROVIDER'
            THEN prc.proc_prov_taxnmy_id
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_rendering_std_taxonomy,
    /* prov_rendering_vendor_specialty */
    CASE
        WHEN prc.proc_prov_speclty_id_qual = 'RENDERING_PROVIDER'
            THEN prc.proc_prov_speclty_id
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_rendering_vendor_specialty,
    CAST(NULL AS STRING)                                                                    AS prov_billing_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_billing_tax_id,
    CAST(NULL AS STRING)                                                                    AS prov_billing_dea_id,
    CAST(NULL AS STRING)                                                                    AS prov_billing_ssn,
    CAST(NULL AS STRING)                                                                    AS prov_billing_state_license,
    CAST(NULL AS STRING)                                                                    AS prov_billing_upin,
    CAST(NULL AS STRING)                                                                    AS prov_billing_commercial_id,
    CAST(NULL AS STRING)                                                                    AS prov_billing_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_billing_name_2,
    CAST(NULL AS STRING)                                                                    AS prov_billing_address_1,
    CAST(NULL AS STRING)                                                                    AS prov_billing_address_2,
    CAST(NULL AS STRING)                                                                    AS prov_billing_city,
    CAST(NULL AS STRING)                                                                    AS prov_billing_state,
    CAST(NULL AS STRING)                                                                    AS prov_billing_zip,
    CAST(NULL AS STRING)                                                                    AS prov_billing_std_taxonomy,
    CAST(NULL AS STRING)                                                                    AS prov_billing_vendor_specialty,
    CAST(NULL AS STRING)                                                                    AS prov_referring_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_referring_tax_id,
    CAST(NULL AS STRING)                                                                    AS prov_referring_dea_id,
    CAST(NULL AS STRING)                                                                    AS prov_referring_ssn,
    CAST(NULL AS STRING)                                                                    AS prov_referring_state_license,
    CAST(NULL AS STRING)                                                                    AS prov_referring_upin,
    CAST(NULL AS STRING)                                                                    AS prov_referring_commercial_id,
    CAST(NULL AS STRING)                                                                    AS prov_referring_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_referring_name_2,
    CAST(NULL AS STRING)                                                                    AS prov_referring_address_1,
    CAST(NULL AS STRING)                                                                    AS prov_referring_address_2,
    CAST(NULL AS STRING)                                                                    AS prov_referring_city,
    CAST(NULL AS STRING)                                                                    AS prov_referring_state,
    CAST(NULL AS STRING)                                                                    AS prov_referring_zip,
    CAST(NULL AS STRING)                                                                    AS prov_referring_std_taxonomy,
    CAST(NULL AS STRING)                                                                    AS prov_referring_vendor_specialty,
    CAST(NULL AS STRING)                                                                    AS prov_facility_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_facility_tax_id,
    CAST(NULL AS STRING)                                                                    AS prov_facility_dea_id,
    CAST(NULL AS STRING)                                                                    AS prov_facility_ssn,
    CAST(NULL AS STRING)                                                                    AS prov_facility_state_license,
    CAST(NULL AS STRING)                                                                    AS prov_facility_upin,
    CAST(NULL AS STRING)                                                                    AS prov_facility_commercial_id,
    CAST(NULL AS STRING)                                                                    AS prov_facility_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_facility_name_2,
    CAST(NULL AS STRING)                                                                    AS prov_facility_address_1,
    CAST(NULL AS STRING)                                                                    AS prov_facility_address_2,
    CAST(NULL AS STRING)                                                                    AS prov_facility_city,
    CAST(NULL AS STRING)                                                                    AS prov_facility_state,
    CAST(NULL AS STRING)                                                                    AS prov_facility_zip,
    CAST(NULL AS STRING)                                                                    AS prov_facility_std_taxonomy,
    CAST(NULL AS STRING)                                                                    AS prov_facility_vendor_specialty,
    CAST(NULL AS STRING)                                                                    AS cob_payer_vendor_id_1,
    CAST(NULL AS STRING)                                                                    AS cob_payer_seq_code_1,
    CAST(NULL AS STRING)                                                                    AS cob_payer_hpid_1,
    CAST(NULL AS STRING)                                                                    AS cob_payer_claim_filing_ind_code_1,
    CAST(NULL AS STRING)                                                                    AS cob_ins_type_code_1,
    CAST(NULL AS STRING)                                                                    AS cob_payer_vendor_id_2,
    CAST(NULL AS STRING)                                                                    AS cob_payer_seq_code_2,
    CAST(NULL AS STRING)                                                                    AS cob_payer_hpid_2,
    CAST(NULL AS STRING)                                                                    AS cob_payer_claim_filing_ind_code_2,
    CAST(NULL AS STRING)                                                                    AS cob_ins_type_code_2,
    CAST(NULL AS STRING)                                                                    AS vendor_test_id,
    CAST(NULL AS STRING)                                                                    AS vendor_test_name,
    CAST(NULL AS DATE)                                                                      AS claim_transaction_date,
    CAST(NULL AS STRING)                                                                    AS claim_transaction_date_qual,
    CAST(NULL AS FLOAT)                                                                     AS claim_transaction_amount,
    CAST(NULL AS STRING)                                                                    AS claim_transaction_amount_qual, 
    CAST(NULL AS STRING)                                                                    AS medical_claim_link_text,
    CAST(NULL AS STRING)                                                                    AS emr_link_text,
    CAST(NULL AS STRING)                                                                    AS logical_delete_reason, 
    'nextgen'                                                                               AS part_provider,
    prc.part_mth                                                                            AS part_best_date
 FROM hvm_emr_proc_v10 prc
WHERE prc.part_hvm_vdr_feed_id = '35'
  AND prc.hvid IS NOT NULL
  AND
    (
        prc.proc_cd IS NOT NULL
     OR prc.proc_diag_cd IS NOT NULL
    )
  /* Only retrieve where the row wasn't already loaded in Step 1. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM hvm_emr_enc_v08 enc
        WHERE prc.part_hvm_vdr_feed_id = '35'
          AND enc.part_hvm_vdr_feed_id = '35'
          AND prc.hvid IS NOT NULL
          AND enc.hvid IS NOT NULL
          AND
            (
                prc.proc_cd IS NOT NULL
             OR prc.proc_diag_cd IS NOT NULL
            )
          AND enc.enc_start_dt IS NOT NULL
          AND COALESCE(enc.hv_enc_id, 'EMPTY') = COALESCE(prc.hv_enc_id, 'UNPOPULATED')
    )
