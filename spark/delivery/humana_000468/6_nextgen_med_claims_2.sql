SELECT DISTINCT
    enc.row_id                                                                              AS record_id,
    enc.hv_enc_id                                                                           AS claim_id,
    enc.hvid                                                                                AS hvid,
    enc.crt_dt                                                                              AS created,
    '7'                                                                                     AS model_version,
    enc.data_set_nm                                                                         AS data_set,
    CAST(enc.hvm_vdr_feed_id AS STRING)                                                     AS data_feed,
    CAST(enc.hvm_vdr_id AS STRING)                                                          AS data_vendor,
    enc.src_vrsn_id                                                                         AS source_version,
    enc.vdr_org_id                                                                          AS vendor_org_id,
    enc.ptnt_gender_cd                                                                      AS patient_gender,
    enc.ptnt_age_num                                                                        AS patient_age,
    CAST(enc.ptnt_birth_yr AS STRING)                                                       AS patient_year_of_birth,
    enc.ptnt_zip3_cd                                                                        AS patient_zip3,
    enc.ptnt_state_cd                                                                       AS patient_state,
    NULL                                                                                    AS claim_type,
    NULL                                                                                    AS date_received,
    enc.enc_start_dt                                                                        AS date_service, 
    NULL                                                                                    AS date_service_end,
    NULL                                                                                    AS inst_date_admitted,
    NULL                                                                                    AS inst_date_discharged,
    NULL                                                                                    AS inst_admit_type_std_id,
    NULL                                                                                    AS inst_admit_type_vendor_id,
    NULL                                                                                    AS inst_admit_type_vendor_desc,
    NULL                                                                                    AS inst_admit_source_std_id,
    NULL                                                                                    AS inst_admit_source_vendor_id,
    NULL                                                                                    AS inst_admit_source_vendor_desc,
    NULL                                                                                    AS inst_discharge_status_std_id,
    NULL                                                                                    AS inst_discharge_status_vendor_id,
    NULL                                                                                    AS inst_discharge_status_vendor_desc,
    NULL                                                                                    AS inst_type_of_bill_std_id,
    NULL                                                                                    AS inst_type_of_bill_vendor_id,
    NULL                                                                                    AS inst_type_of_bill_vendor_desc,
    NULL                                                                                    AS inst_drg_std_id,
    NULL                                                                                    AS inst_drg_vendor_id,
    NULL                                                                                    AS inst_drg_vendor_desc,
    NULL                                                                                    AS place_of_service_std_id,
    NULL                                                                                    AS place_of_service_vendor_id,
    NULL                                                                                    AS place_of_service_vendor_desc,
    NULL                                                                                    AS service_line_number,
    dgn.diag_cd                                                                             AS diagnosis_code,
    NULL                                                                                    AS diagnosis_code_qual,
    NULL                                                                                    AS diagnosis_priority,
    NULL                                                                                    AS admit_diagnosis_ind,
    NULL                                                                                    AS procedure_code,
    NULL                                                                                    AS procedure_code_qual,
    NULL                                                                                    AS principal_proc_ind,
    NULL                                                                                    AS procedure_units_billed,
    NULL                                                                                    AS procedure_units_paid,
    NULL                                                                                    AS procedure_modifier_1,
    NULL                                                                                    AS procedure_modifier_2,
    NULL                                                                                    AS procedure_modifier_3,
    NULL                                                                                    AS procedure_modifier_4,
    NULL                                                                                    AS revenue_code,
    NULL                                                                                    AS ndc_code,
    NULL                                                                                    AS medical_coverage_type,
    NULL                                                                                    AS line_charge,
    NULL                                                                                    AS line_allowed,
    NULL                                                                                    AS total_charge,
    NULL                                                                                    AS total_allowed,
    NULL                                                                                    AS prov_rendering_npi,
    NULL                                                                                    AS prov_billing_npi,
    NULL                                                                                    AS prov_referring_npi,
    NULL                                                                                    AS prov_facility_npi,
    NULL                                                                                    AS payer_vendor_id,
    NULL                                                                                    AS payer_name,
    NULL                                                                                    AS payer_parent_name,
    NULL                                                                                    AS payer_org_name,
    NULL                                                                                    AS payer_plan_id,
    NULL                                                                                    AS payer_plan_name,
    NULL                                                                                    AS payer_type,
    NULL                                                                                    AS prov_rendering_vendor_id,
    NULL                                                                                    AS prov_rendering_tax_id,
    NULL                                                                                    AS prov_rendering_dea_id,
    NULL                                                                                    AS prov_rendering_ssn,
    NULL                                                                                    AS prov_rendering_state_license,
    NULL                                                                                    AS prov_rendering_upin,
    NULL                                                                                    AS prov_rendering_commercial_id,
    NULL                                                                                    AS prov_rendering_name_1,
    NULL                                                                                    AS prov_rendering_name_2,
    NULL                                                                                    AS prov_rendering_address_1,
    NULL                                                                                    AS prov_rendering_address_2,
    NULL                                                                                    AS prov_rendering_city,
    NULL                                                                                    AS prov_rendering_state,
    NULL                                                                                    AS prov_rendering_zip,
    NULL                                                                                    AS prov_rendering_std_taxonomy,
    NULL                                                                                    AS prov_rendering_vendor_specialty,
    NULL                                                                                    AS prov_billing_vendor_id,
    NULL                                                                                    AS prov_billing_tax_id,
    NULL                                                                                    AS prov_billing_dea_id,
    NULL                                                                                    AS prov_billing_ssn,
    NULL                                                                                    AS prov_billing_state_license,
    NULL                                                                                    AS prov_billing_upin,
    NULL                                                                                    AS prov_billing_commercial_id,
    NULL                                                                                    AS prov_billing_name_1,
    NULL                                                                                    AS prov_billing_name_2,
    NULL                                                                                    AS prov_billing_address_1,
    NULL                                                                                    AS prov_billing_address_2,
    NULL                                                                                    AS prov_billing_city,
    NULL                                                                                    AS prov_billing_state,
    NULL                                                                                    AS prov_billing_zip,
    NULL                                                                                    AS prov_billing_std_taxonomy,
    NULL                                                                                    AS prov_billing_vendor_specialty,
    NULL                                                                                    AS prov_referring_vendor_id,
    NULL                                                                                    AS prov_referring_tax_id,
    NULL                                                                                    AS prov_referring_dea_id,
    NULL                                                                                    AS prov_referring_ssn,
    NULL                                                                                    AS prov_referring_state_license,
    NULL                                                                                    AS prov_referring_upin,
    NULL                                                                                    AS prov_referring_commercial_id,
    NULL                                                                                    AS prov_referring_name_1,
    NULL                                                                                    AS prov_referring_name_2,
    NULL                                                                                    AS prov_referring_address_1,
    NULL                                                                                    AS prov_referring_address_2,
    NULL                                                                                    AS prov_referring_city,
    NULL                                                                                    AS prov_referring_state,
    NULL                                                                                    AS prov_referring_zip,
    NULL                                                                                    AS prov_referring_std_taxonomy,
    NULL                                                                                    AS prov_referring_vendor_specialty,
    NULL                                                                                    AS prov_facility_vendor_id,
    NULL                                                                                    AS prov_facility_tax_id,
    NULL                                                                                    AS prov_facility_dea_id,
    NULL                                                                                    AS prov_facility_ssn,
    NULL                                                                                    AS prov_facility_state_license,
    NULL                                                                                    AS prov_facility_upin,
    NULL                                                                                    AS prov_facility_commercial_id,
    NULL                                                                                    AS prov_facility_name_1,
    NULL                                                                                    AS prov_facility_name_2,
    NULL                                                                                    AS prov_facility_address_1,
    NULL                                                                                    AS prov_facility_address_2,
    NULL                                                                                    AS prov_facility_city,
    NULL                                                                                    AS prov_facility_state,
    NULL                                                                                    AS prov_facility_zip,
    NULL                                                                                    AS prov_facility_std_taxonomy,
    NULL                                                                                    AS prov_facility_vendor_specialty,
    NULL                                                                                    AS cob_payer_vendor_id_1,
    NULL                                                                                    AS cob_payer_seq_code_1,
    NULL                                                                                    AS cob_payer_hpid_1,
    NULL                                                                                    AS cob_payer_claim_filing_ind_code_1,
    NULL                                                                                    AS cob_ins_type_code_1,
    NULL                                                                                    AS cob_payer_vendor_id_2,
    NULL                                                                                    AS cob_payer_seq_code_2,
    NULL                                                                                    AS cob_payer_hpid_2,
    NULL                                                                                    AS cob_payer_claim_filing_ind_code_2,
    NULL                                                                                    AS cob_ins_type_code_2,
    NULL                                                                                    AS vendor_test_id,
    NULL                                                                                    AS vendor_test_name,
    NULL                                                                                    AS claim_transaction_date,
    NULL                                                                                    AS claim_transaction_date_qual,
    NULL                                                                                    AS claim_transaction_amount,
    NULL                                                                                    AS claim_transaction_amount_qual,
    NULL                                                                                    AS medical_claim_link_text,
    NULL                                                                                    AS emr_link_text,
    NULL                                                                                    AS logical_delete_reason,
    'nextgen'                                                                               AS part_provider, 
    CONCAT(enc.part_mth, '-01')                                                             AS part_processdate
 FROM 
(
    SELECT *
     FROM hvm_emr_enc enc1
    WHERE enc1.part_hvm_vdr_feed_id = '35'
      AND enc1.hvid IS NOT NULL
      AND enc1.enc_start_dt IS NOT NULL
      -- Reduce the encounter data to one row per hv_enc_id --  the most recent.
      -- Eliminate rows where another row exists with a later create date,
      -- with a "larger" dataset name, or the same dataset name and a larger row ID.
      AND NOT EXISTS
        (
            SELECT 1
             FROM hvm_emr_enc enc2
            WHERE enc2.part_hvm_vdr_feed_id = '35'
              AND enc2.hvid IS NOT NULL
              AND enc2.enc_start_dt IS NOT NULL
              
              AND enc1.part_hvm_vdr_feed_id = '35'
              AND enc1.hvid IS NOT NULL
              AND enc1.enc_start_dt IS NOT NULL
              
              AND enc1.hv_enc_id = enc2.hv_enc_id
              
              AND
                -- Later create date, larger data_set_nm.
                (
                    enc2.crt_dt > enc1.crt_dt
                 OR enc2.data_set_nm > enc1.data_set_nm
                 OR
                 -- Same dataset, but a larger row_id.
                    (
                        enc2.data_set_nm = enc1.data_set_nm
                    AND enc2.row_id > enc1.row_id
                    )
                )
        )
) enc
INNER JOIN hvm_emr_diag dgn
   ON dgn.part_hvm_vdr_feed_id = '35'
  AND dgn.hvid IS NOT NULL
  AND dgn.diag_cd IS NOT NULL
  AND COALESCE(dgn.hv_enc_id, 'EMPTY') = COALESCE(enc.hv_enc_id, 'UNPOPULATED')
 LEFT OUTER JOIN dw.ref_gen_ref ref
   ON ref.gen_ref_domn_nm = 'hvm_emr_enc.enc_typ_cd'
  AND ref.hvm_vdr_feed_id = 35
  AND ref.whtlst_flg = 'Y'
  AND ref.gen_ref_cd = COALESCE(UPPER(enc.enc_typ_cd), 'empty')
WHERE
-- Either the Encounter Type Code is empty, or it's in the whitelist.
    (
        enc.enc_typ_cd IS NULL
     OR ref.gen_ref_cd IS NOT NULL
    )
  -- Only retrieve if not already loaded from Procedure in Step 1.
  AND NOT EXISTS
    (
        SELECT 1
         FROM hvm_emr_proc prc
        WHERE dgn.part_hvm_vdr_feed_id = '35'
          AND prc.part_hvm_vdr_feed_id = '35'
          AND dgn.hvid IS NOT NULL
          AND prc.hvid IS NOT NULL
          AND dgn.diag_cd IS NOT NULL
          AND prc.proc_diag_cd IS NOT NULL
          AND COALESCE(dgn.hv_enc_id, 'EMPTY') = COALESCE(prc.hv_enc_id, 'UNPOPULATED')
          AND COALESCE(dgn.diag_cd, 'EMPTY') = COALESCE(prc.proc_diag_cd, 'UNPOPULATED')
    )
