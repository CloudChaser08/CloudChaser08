
  SELECT
    MONOTONICALLY_INCREASING_ID()                                               AS row_id,
    CONCAT('265', '_', COALESCE(txn.src_era_svc_id, src_svc_id))                AS hv_medcl_clm_pymt_dtl_id,
    CURRENT_DATE                                                                AS crt_dt,
    '07'                                                                        AS mdl_vrsn_num,
    3                                                                           AS hvm_vdr_id,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]                                                                 AS data_set_nm,
    265                                                                         AS hvm_vdr_feed_id,

    txn.src_era_claim_id                                                        AS vdr_medcl_clm_pymt_sumry_id,
    CASE
      WHEN txn.src_era_claim_id IS NOT NULL THEN 'SUPPLIER_CLAIM_PAYMENT_NUMBER'
    ELSE NULL
    END                                                                         AS vdr_medcl_clm_pymt_sumry_id_qual,

    txn.src_era_svc_id                                                          AS vdr_medcl_clm_pymt_dtl_id,
    CASE
      WHEN txn.src_era_svc_id IS NOT NULL THEN 'CLAIM_PAYMENT_NUMBER'
    ELSE NULL
    END                                                                         AS vdr_medcl_clm_pymt_dtl_id_qual,

    CASE
      WHEN CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d'))   AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
        OR CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d'))   AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE)
      THEN  NULL
      --------------------------------------------------------------------------------------------
      ------------- If the service line start date is empty - populate from the claim statement start date.
      --------------------------------------------------------------------------------------------
      ELSE CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d'))   AS DATE)
    END                                                                        AS svc_ln_start_dt,

    CASE
      WHEN CAST(COALESCE(EXTRACT_DATE(txn.svc_to_dt,   '%Y%m%d'), EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d'))   AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
        OR CAST(COALESCE(EXTRACT_DATE(txn.svc_to_dt,   '%Y%m%d'), EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d'))   AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE)
      THEN  NULL
      ------------------------------------------------------------------------------------------------------------------------------------------
      --------- If the service line end date is NULL and claim statement end date > service_from_date (service line START date) then do not populate
      -----------------------------------------------------------------------------------------------------------------------------------------
      WHEN txn.svc_to_dt IS NULL
          AND CAST(EXTRACT_DATE(txn.stmnt_to_dt, '%Y-%m-%d') AS DATE)  <  CAST(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d') AS DATE)
      THEN NULL
      ------------------------------------------------------------------------------------------------------------------------------------------
      --------- If the service line end date is NULL and claim statement_to_date < claim statement_from_date then do not populate
      -----------------------------------------------------------------------------------------------------------------------------------------
      WHEN txn.svc_to_dt IS NULL
           AND  CAST(EXTRACT_DATE(txn.stmnt_to_dt, '%Y-%m-%d') AS DATE) < CAST(EXTRACT_DATE(txn.stmnt_from_dt, '%Y-%m-%d') AS DATE)
      THEN NULL
      ELSE CAST(COALESCE(EXTRACT_DATE(txn.svc_to_dt,   '%Y%m%d'), EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d'))   AS DATE)
    END                                                                       AS svc_ln_end_dt,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_npi
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE CLEAN_UP_NPI_CODE(txn.svc_rendr_provdr_npi)
    END                                                                       AS rndrg_prov_npi,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_state_lic_id
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.svc_rendr_provdr_stlc_nbr
    END                                                                       AS rndrg_prov_state_lic_id,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_upin
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.svc_rendr_provdr_upin
    END                                                                       AS rndrg_prov_upin,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_comrcl_id
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.svc_rendr_provdr_comm_nbr
    END                                                                       AS rndrg_prov_comrcl_id,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    adjctd_proc_cd and adjctd_proc_cd_modfr
    --------------------------------------------------------------------------------------------------------------------------------
    CLEAN_UP_PROCEDURE_CODE(COALESCE(UPPER(txn.adjtd_proc_cd), ''))           AS adjctd_proc_cd,

    CASE
        WHEN txn.adjtd_proc_cd IS NOT NULL THEN txn.adjtd_proc_cd_qual
        ELSE NULL
    END                                                                       AS adjctd_proc_cd_qual,

    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.adjtd_proc_modfr_1), 1, 2))   AS adjctd_proc_cd_1_modfr,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.adjtd_proc_modfr_2), 1, 2))   AS adjctd_proc_cd_2_modfr,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.adjtd_proc_modfr_3), 1, 2))   AS adjctd_proc_cd_3_modfr,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.adjtd_proc_modfr_4), 1, 2))   AS adjctd_proc_cd_4_modfr,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    orig_submtd_proc_cd and orig_submtd_proc_cd_modfr
    --------------------------------------------------------------------------------------------------------------------------------
    CLEAN_UP_ALPHANUMERIC_CODE(UPPER(txn.submd_proc_cd))                      AS orig_submtd_proc_cd,
    CASE
        WHEN txn.submd_proc_cd IS NOT NULL THEN txn.adjtd_proc_cd_qual
        ELSE NULL
    END                                                                       AS orig_submtd_proc_cd_qual,

    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.submd_proc_modfr_1), 1, 2))   AS orig_submtd_proc_cd_1_modfr,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.submd_proc_modfr_2), 1, 2))   AS orig_submtd_proc_cd_2_modfr,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.submd_proc_modfr_3), 1, 2))   AS orig_submtd_proc_cd_3_modfr,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.submd_proc_modfr_4), 1, 2))   AS orig_submtd_proc_cd_4_modfr,

    CAST(REPLACE(REPLACE(txn.line_item_charg_amt, ",",""), "$","") AS FLOAT)  AS svc_ln_submtd_chg_amt,
    CAST(REPLACE(REPLACE(txn.line_item_paid_amt , ",",""), "$","") AS FLOAT)  AS svc_ln_prov_pymt_amt,
    txn.revnu_cd                                                              AS rev_cd,
    txn.paid_units                                                            AS paid_svc_unt_cnt,
    txn.origl_units_of_svc_cnt                                                AS orig_svc_unt_cnt,
    --------------------------------------------------------------------------------------------------------------------------------
    -------------------- claim adjustment section
    --------------------------------------------------------------------------------------------------------------------------------
    svc_cas_pvt.svc_ln_adjmt_grp_cd                                                   AS svc_ln_adjmt_grp_cd,
    svc_cas_pvt.svc_ln_adjmt_seq_num                                                  AS svc_ln_adjmt_seq_num,
    svc_cas_pvt.svc_ln_adjmt_rsn_cd                                                   AS svc_ln_adjmt_rsn_cd,
    CAST(REPLACE(REPLACE(svc_cas_pvt.svc_ln_adjmt_amt, ",",""), "$","")AS FLOAT)      AS svc_ln_adjmt_amt,
    --------------------------------------------------------------------------------------------------------------------------------
    MD5(txn.line_item_cntl_nbr)                                                       AS svc_ln_vdr_ctl_num,
    CAST(REPLACE(REPLACE(txn.line_item_allowed_amt, ",",""), "$","")AS FLOAT)         AS svc_ln_suplmtl_amt,
    CASE
        WHEN txn.line_item_allowed_amt IS NOT NULL THEN 'B6'
        ELSE NULL
    END                                                                               AS svc_ln_suplmtl_amt_qual,

   '265'                                                                              AS part_hvm_vdr_feed_id,
    CASE WHEN 0 = LENGTH(TRIM(COALESCE(
                                      ----svc_ln_start_dt
                                      CASE
                                        WHEN CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d'))   AS DATE)
                                             < CAST('{AVAILABLE_START_DATE}' AS DATE)
                                          OR CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d'))   AS DATE)
                                             > CAST('{VDR_FILE_DT}'                 AS DATE)
                                        THEN  NULL
                                        --------------------------------------------------------------------------------------------
                                        ------------- If the service line start date is empty - populate from the claim statement start date.
                                        --------------------------------------------------------------------------------------------
                                        ELSE CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d'))   AS DATE)
                                      END
                                      ,'')))
         THEN  '0_PREDATES_HVM_HISTORY'
         ELSE
            SUBSTR(CAST(COALESCE(EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'), EXTRACT_DATE(txn.stmnt_from_dt, '%Y-%m-%d'))   AS DATE)
                   , 1, 7)
    END              AS part_mth
FROM  txn
LEFT OUTER JOIN claimremedi_svc_cas_pvt svc_cas_pvt
on txn.src_era_svc_id =  svc_cas_pvt.src_era_svc_id
--limit 10
