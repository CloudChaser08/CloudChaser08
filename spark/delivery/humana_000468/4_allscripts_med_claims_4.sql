SELECT 
    dgn.row_id                                                                              AS record_id,
    dgn.hv_diag_id                                                                          AS claim_id,
    dgn.hvid                                                                                AS hvid,
    dgn.crt_dt                                                                              AS created,
    '7'                                                                                     AS model_version,
    dgn.data_set_nm                                                                         AS data_set,
    CAST(dgn.hvm_vdr_feed_id AS STRING)                                                     AS data_feed,
    CAST(dgn.hvm_vdr_id AS STRING)                                                          AS data_vendor,
    dgn.src_vrsn_id                                                                         AS source_version,
    dgn.vdr_org_id                                                                          AS vendor_org_id,
    dgn.ptnt_gender_cd                                                                      AS patient_gender,
    dgn.ptnt_age_num                                                                        AS patient_age,
    CAST(dgn.ptnt_birth_yr AS STRING)                                                       AS patient_year_of_birth,
    dgn.ptnt_zip3_cd                                                                        AS patient_zip3,
    dgn.ptnt_state_cd                                                                       AS patient_state,
    NULL                                                                                    AS claim_type,
    dgn.data_captr_dt                                                                       AS date_received,
    -- COALESCE the primary date with other dates available.
    COALESCE
        (
            dgn.diag_dt, 
            dgn.enc_dt, 
            dgn.diag_onset_dt, 
            dgn.diag_resltn_dt
        )                                                                                   AS date_service, 
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
    dgn.diag_cd_qual                                                                        AS diagnosis_code_qual,
    TRIM
        (
        CONCAT
            (
                CASE WHEN 0 <> LENGTH(TRIM(COALESCE(dgn.diag_stat_cd, '')))
                      AND TRIM(UPPER(COALESCE(dgn.diag_stat_nm, 'EMPTY'))) NOT IN
                            (
                                '*',
                                'UNDEFINED',
                                'UNKNOWN'
                            )
                          THEN CONCAT
                                (
                                    TRIM(UPPER(COALESCE(dgn.diag_stat_cd, ''))),
                                    ' '
                                )
                     ELSE ''
                 END,
                CASE WHEN 0 <> LENGTH(TRIM(COALESCE(dgn.diag_stat_nm, '')))
                      AND TRIM(UPPER(COALESCE(dgn.diag_stat_nm, 'EMPTY'))) IN
                            (
                                'CHRONIC',
                                'ACUTE',
                                'HEALTH MAINTENANCE CONCEPT'
                            )
                          THEN CONCAT
                                (
                                    TRIM(UPPER(COALESCE(dgn.diag_stat_nm, ''))),
                                    ' '
                                )
                     ELSE ''
                 END,
                CASE WHEN 0 <> LENGTH(TRIM(COALESCE(dgn.diag_stat_desc, '')))
                      AND TRIM(UPPER(COALESCE(dgn.diag_stat_desc, 'EMPTY'))) <> 'XYXYXY'
                          THEN CONCAT
                                (
                                    TRIM(UPPER(COALESCE(dgn.diag_stat_desc, ''))),
                                    ' '
                                )
                     ELSE ''
                 END
            )        
        )                                                                                   AS diagnosis_priority,
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
    dgn.diag_rndrg_prov_state_cd                                                            AS prov_rendering_state,
    NULL                                                                                    AS prov_rendering_zip, 
    dgn.diag_rndrg_prov_taxnmy_id                                                           AS prov_rendering_std_taxonomy,
    dgn.diag_rndrg_prov_speclty_id                                                          AS prov_rendering_vendor_specialty,
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
    dgn.rec_stat_cd                                                                         AS logical_delete_reason, 
    'allscripts'                                                                            AS part_provider, 
    CONCAT(dgn.part_mth, '-01')                                                             AS part_processdate
 FROM hvm_emr_diag dgn
WHERE dgn.part_hvm_vdr_feed_id = '25'
  AND dgn.hvid IS NOT NULL
  AND dgn.diag_cd IS NOT NULL
  -- The following line was added 9/5/18 as per Reyna.
  -- Eliminate rows that were entered in error.
  AND UPPER(COALESCE(dgn.rec_stat_cd, 'EMPTY')) NOT LIKE '%ENTERED IN ERROR%'
  -- Only retrieve if not already loaded from Procedure in Steps 1 or 3.
  AND NOT EXISTS
    (
        SELECT 1
         FROM hvm_emr_proc prc
        WHERE dgn.part_hvm_vdr_feed_id = '25'
          AND prc.part_hvm_vdr_feed_id = '25'
          AND dgn.hvid IS NOT NULL
          AND prc.hvid IS NOT NULL
          AND dgn.diag_cd IS NOT NULL
          AND prc.proc_diag_cd IS NOT NULL
          AND COALESCE(dgn.hv_enc_id, 'EMPTY') = COALESCE(prc.hv_enc_id, 'UNPOPULATED')
          AND COALESCE(dgn.diag_cd, 'EMPTY') = COALESCE(prc.proc_diag_cd, 'UNPOPULATED')
    )
  -- Only retrieve where the row wasn't already loaded in Step 2.
  AND NOT EXISTS
    (
        SELECT 1
         FROM hvm_emr_enc enc
        WHERE dgn.part_hvm_vdr_feed_id = '25'
          AND enc.part_hvm_vdr_feed_id = '25'
          AND dgn.hvid IS NOT NULL
          AND enc.hvid IS NOT NULL
          AND dgn.diag_cd IS NOT NULL
          AND enc.enc_start_dt IS NOT NULL
          AND COALESCE(enc.hv_enc_id, 'EMPTY') = COALESCE(dgn.hv_enc_id, 'UNPOPULATED')
    )
