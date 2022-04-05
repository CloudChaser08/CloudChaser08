
SELECT
  CONCAT('265', '_',txn.src_era_claim_id)                                   AS hv_medcl_clm_pymt_sumry_id,
  CURRENT_DATE                                                              AS crt_dt,
  '07'                                                                      AS mdl_vrsn_num,
  3                                                                         AS hvm_vdr_id,
  SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]                                                           AS data_set_nm,
  265                                                                       AS hvm_vdr_feed_id,

  txn.src_era_claim_id                                                      AS vdr_medcl_clm_pymt_sumry_id,
  CASE
    WHEN txn.src_era_claim_id IS NOT NULL THEN 'SUPPLIER_CLAIM_PAYMENT_NUMBER'
  ELSE NULL
  END                                                                      AS vdr_medcl_clm_pymt_sumry_id_qual,
  --------- clm_stmt_perd_start_dt
  CASE  ------------  If clm.statement_from_date is BEFORE (less than)  min_max_dt.min_service_from_date  or it is NULL, then populate serviceLine.min_svc_from_dt
    WHEN CAST(EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d') AS DATE) > CAST(EXTRACT_DATE(serviceLine.min_svc_from_dt, '%Y%m%d') AS DATE)
         OR txn.stmnt_from_dt IS NULL
    THEN
      CASE
        WHEN CAST(EXTRACT_DATE(serviceLine.min_svc_from_dt, '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}'     AS DATE)
          OR CAST(EXTRACT_DATE(serviceLine.min_svc_from_dt, '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE)
        THEN NULL
        ELSE CAST(EXTRACT_DATE(serviceLine.min_svc_from_dt, '%Y%m%d') AS DATE)
      END
  ELSE   ---------     Else populate txn.stmnt_from_dt
    CASE
      WHEN CAST(EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}'     AS DATE)
        OR CAST(EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                    AS DATE)
      THEN NULL
      ELSE CAST(EXTRACT_DATE(txn.stmnt_from_dt , '%Y-%m-%d') AS DATE)
    END
  END                                                                       AS clm_stmt_perd_start_dt,


  ------------ clm_stmt_perd_end_dt
  CASE    ----If claim end date is AFTER  the last service line end date or clm_stmt_perd_end_dt is NULL, then populate serviceLine.max_svc_to_dt
    WHEN CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) < CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE)
      OR txn.stmnt_to_dt IS NULL
    THEN
      CASE
        WHEN CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}'     AS DATE)
          OR CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                     AS DATE)
        THEN NULL
        ELSE   CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE)
      END
    ELSE   ----      Else populate txn.stmnt_to_dt
      CASE
        WHEN CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}'     AS DATE)
          OR CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) > CAST('{VDR_FILE_DT}'                     AS DATE)
        THEN NULL
        ELSE  CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE)
      END
  END                                                                     AS clm_stmt_perd_end_dt,


  CASE
    WHEN CAST(EXTRACT_DATE(COALESCE(txn.claim_received_dt, txn.edi_interchange_creation_dt), '%Y-%m-%d')   AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
      OR CAST(EXTRACT_DATE(COALESCE(txn.claim_received_dt, txn.edi_interchange_creation_dt), '%Y-%m-%d')   AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE)
    THEN  NULL
    ELSE CAST(EXTRACT_DATE(COALESCE(txn.claim_received_dt, txn.edi_interchange_creation_dt), '%Y-%m-%d')   AS DATE)
    END                                                                   AS payr_clm_recpt_dt,

  COALESCE(txn.payer_id, txn.payer_naic_id, txn.payer_cms_plan_id)        AS payr_id,
  CASE
    WHEN txn.payer_id          IS NOT NULL THEN 'PAYER_ID'
    WHEN txn.payer_naic_id     IS NOT NULL THEN 'PAYER_NAIC_ID'
    WHEN txn.payer_cms_plan_id IS NOT NULL THEN 'PAYER_CMS_PLAN_ID'
    ELSE NULL
  END                                                                     AS payr_id_qual,
  CASE
    WHEN txn.payer_nm IS NOT NULL AND txn.payer_cms_plan_id IS NOT NULL THEN CONCAT(txn.payer_nm, ' - ', txn.payer_cms_plan_id)
    WHEN txn.payer_nm IS     NULL AND txn.payer_cms_plan_id IS NOT NULL THEN txn.payer_cms_plan_id
    WHEN txn.payer_nm IS NOT NULL AND txn.payer_cms_plan_id IS     NULL THEN txn.payer_nm
    ELSE NULL
  END                                                                     AS payr_nm,

  --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_npi
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE CLEAN_UP_NPI_CODE(txn.payee_npi)
     END                                                                 AS bllg_prov_npi,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_tax_id
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.payee_tax_id
    END                                                                   AS bllg_prov_tax_id,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_1_nm
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.payee_nm
    END                                                                   AS bllg_prov_1_nm,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_addr_1_txt
    --------------------------------------------------------------------------------------------------------------------------------
   CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.payee_addr_1
    END                                                                   AS bllg_prov_addr_1_txt,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_addr_2_txt
    --------------------------------------------------------------------------------------------------------------------------------
   CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.payee_addr_2
    END                                                                   AS bllg_prov_addr_2_txt,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_city_nm
    --------------------------------------------------------------------------------------------------------------------------------
     CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.payee_addr_city
    END                                                                   AS bllg_prov_city_nm,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_state_cd
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE VALIDATE_STATE_CODE(UPPER(txn.payee_addr_state))
    END                                                               AS bllg_prov_state_cd,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    bllg_prov_zip_cd
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) NOT IN ('I', 'P')
             THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
             LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        ELSE txn.payee_addr_zip
    END                                                                   AS bllg_prov_zip_cd,

    CAST(REPLACE(REPLACE(txn.tot_actual_provdr_paymt_amt, ",",""), "$","") AS FLOAT)  AS clm_prov_pymt_amt,
    CASE
        WHEN txn.tot_actual_provdr_paymt_amt IS NOT NULL THEN  txn.txn_handling_cd
        ELSE NULL
    END                                                                     AS clm_prov_pymt_amt_qual,

    CASE
      WHEN CAST(EXTRACT_DATE(txn.paymt_eff_dt, '%Y-%m-%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
        OR CAST(EXTRACT_DATE(txn.paymt_eff_dt, '%Y-%m-%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE)
      THEN  NULL
    ELSE CAST(EXTRACT_DATE(txn.paymt_eff_dt, '%Y-%m-%d') AS DATE)
    END                                                                      AS clm_prov_pymt_dt,

    txn.claim_sts_cd			                                             AS clm_stat_cd,
    CAST(REPLACE(REPLACE(txn.tot_claim_charg_amt, ",",""), "$","") AS FLOAT) AS clm_submtd_chg_amt,
    CAST(REPLACE(REPLACE(txn.tot_paid_amt       , ",",""), "$","") AS FLOAT) AS clm_pymt_amt,
    CAST(REPLACE(REPLACE(txn.patnt_rspbty_amt   , ",",""), "$","") AS FLOAT) AS ptnt_respbty_amt,
    txn.payer_claim_flng_ind_cd	                                             AS medcl_covrg_typ_cd,
    MD5(txn.payer_claim_cntl_nbr)	                                         AS payr_clm_ctl_num,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    pos_cd
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) <> 'P'                  THEN NULL
        WHEN LPAD(txn.fclty_type_pos_cd, 2, '0') in ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN '99'
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X'))  =  'P'  AND txn.fclty_type_pos_cd IS NOT NULL
          THEN LPAD(txn.fclty_type_pos_cd, 2, '0')
    END                                                                  AS pos_cd,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    instnl_typ_of_bll_cd
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) <> 'I'  THEN NULL
      ELSE CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, ''))
    END                                                               AS instnl_typ_of_bll_cd,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    drg_cd
    --------------------------------------------------------------------------------------------------------------------------------
    nullify_drg_blacklist(txn.drg_cd)                                         AS drg_cd,
    txn.drg_amt                                                               AS drg_weight_num,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------   rndrg_prov_npi
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
      WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
                       LPAD(txn.fclty_type_pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN NULL
      ELSE CLEAN_UP_NPI_CODE(txn.rendr_provdr_npi)
    END                                                                      AS rndrg_prov_npi,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_state_lic_id
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
      WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
                       LPAD(txn.fclty_type_pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN NULL
      ELSE  txn.rendr_provdr_stlc_nbr
    END                                                                       AS rndrg_prov_state_lic_id,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_upin
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
      WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
                       LPAD(txn.fclty_type_pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN NULL
      ELSE  txn.rendr_provdr_upin
    END                                                                      AS rndrg_prov_upin,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------    rndrg_prov_comrcl_id
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
      WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
                       LPAD(txn.fclty_type_pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN NULL
      ELSE txn.rendr_provdr_comm_nbr
    END                                                                      AS rndrg_prov_comrcl_id,
    --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------   rndrg_prov_1_nm
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
      WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
                       LPAD(txn.fclty_type_pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN NULL
      ELSE  txn.rendr_provdr_last_nm
    END                                                                      AS rndrg_prov_1_nm,
     --------------------------------------------------------------------------------------------------------------------------------
    ------------------------------   rndrg_prov_2_nm
    --------------------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
      WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND
                       LPAD(txn.fclty_type_pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
          THEN NULL
      ELSE  txn.rendr_provdr_first_nm
    END                                                                      AS rndrg_prov_2_nm,

    --------------------------------------------------------------------------------------------------------------------------------
    -------------------- claim adjustment section
    --------------------------------------------------------------------------------------------------------------------------------
    clm_cas_pvt.clm_adjmt_seq_num                                                    AS clm_adjmt_seq_num,
    clm_cas_pvt.clm_adjmt_grp_cd                                                     AS clm_adjmt_grp_cd,
    clm_cas_pvt.clm_adjmt_rsn_cd                                                     AS clm_adjmt_rsn_cd,
    clm_cas_pvt.clm_adjmt_amt                                                        AS clm_adjmt_amt,
    txn.src_claim_id                                                                 AS medcl_clm_lnk_txt,
    txn.claim_type_cd                                                                AS clm_typ_cd,
    '265'                                                                            AS part_hvm_vdr_feed_id,

    CASE
    WHEN 0 = LENGTH(TRIM(COALESCE(  ---clm_stmt_perd_end_dt
                             CASE
                                WHEN CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) < CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE)
                                  OR txn.stmnt_to_dt IS NULL
                                THEN
                                  CASE
                                    WHEN CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE) < CAST('{AVAILABLE_START_DATE}'     AS DATE)
                                      OR CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                     AS DATE)
                                    THEN NULL
                                    ELSE   CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE)
                                  END
                                ELSE
                                  CASE
                                    WHEN CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) < CAST('{AVAILABLE_START_DATE}'     AS DATE)
                                      OR CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) > CAST('{VDR_FILE_DT}'                     AS DATE)
                                    THEN NULL
                                    ELSE  CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE)
                                  END
                              END
                           ,'')))
    THEN  '0_PREDATES_HVM_HISTORY'
    ELSE
         SUBSTR(  ---clm_stmt_perd_end_dt
           CASE
              WHEN CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) < CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE)
                OR txn.stmnt_to_dt IS NULL
              THEN
                CASE
                  WHEN CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE) < CAST('{AVAILABLE_START_DATE}'     AS DATE)
                    OR CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                     AS DATE)
                  THEN NULL
                  ELSE   CAST(EXTRACT_DATE(serviceLine.max_svc_to_dt, '%Y%m%d') AS DATE)
                END
              ELSE
                CASE
                  WHEN CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) < CAST('{AVAILABLE_START_DATE}'     AS DATE)
                    OR CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE) > CAST('{VDR_FILE_DT}'                     AS DATE)
                  THEN NULL
                  ELSE  CAST(EXTRACT_DATE(txn.stmnt_to_dt , '%Y-%m-%d') AS DATE)
                END
            END
         , 1, 7)
    END                                                                   AS part_mth
FROM txn
LEFT OUTER JOIN (SELECT
                     txn_1.src_era_claim_id ,
                     MAX(txn_1.svc_to_dt)   AS max_svc_to_dt,
                     MIN(txn_1.svc_from_dt) AS min_svc_from_dt
                   FROM txn  txn_1 GROUP BY 1 ) serviceLine
ON txn.src_era_claim_id = serviceLine.src_era_claim_id
LEFT OUTER JOIN claimremedi_clm_cas_pvt clm_cas_pvt
on txn.src_era_claim_id =  clm_cas_pvt.src_era_claim_id
--limit 1
