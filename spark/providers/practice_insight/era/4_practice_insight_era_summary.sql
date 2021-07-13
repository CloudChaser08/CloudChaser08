SELECT
  CONCAT('216', '_',txn.src_era_claim_id)                                   AS hv_medcl_clm_pymt_sumry_id,
  CURRENT_DATE                                                              AS crt_dt,
  '07'                                                                      AS mdl_vrsn_num,
  3                                                                         AS hvm_vdr_id,
  data_set_nm                                                               AS data_set_nm,
  216                                                                       AS hvm_vdr_feed_id,
  txn.src_era_claim_id                                                      AS vdr_medcl_clm_pymt_sumry_id,
  txn.claim_type_cd                                                         AS clm_typ_cd,
  CASE 
    WHEN txn.src_era_claim_id IS NOT NULL THEN 'SUPPLIER_CLAIM_PAYMENT_NUMBER' 
  ELSE NULL 
  END                                                             AS vdr_medcl_clm_pymt_sumry_id_qual,
  CAP_DATE(
            CAST(COALESCE(txn.stmnt_from_dt, serviceLine.min_svc_from_dt, txn.stmnt_to_dt, serviceLine.max_svc_to_dt)   AS DATE) ,
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}'                 AS DATE) 
            )                                                               AS clm_stmt_perd_start_dt,
  CAP_DATE(
            CAST(COALESCE(txn.stmnt_to_dt, serviceLine.max_svc_to_dt, txn.stmnt_from_dt, serviceLine.min_svc_from_dt)   AS DATE) ,
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}'                 AS DATE) 
            )                                                               AS clm_stmt_perd_end_dt,

  CAP_DATE(
            CAST(COALESCE(txn.claim_received_dt, txn.edi_interchange_creation_dt)   AS DATE) ,
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}'                 AS DATE) 
              )                                                             AS payr_clm_recpt_dt,
  COALESCE(txn.payer_id, txn.payer_naic_id, txn.payer_cms_plan_id)          AS payr_id,
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
    clean_up_npi_code
    (
        CASE 
            WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
            WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL 
            ELSE txn.payee_npi
        END
    )                                                                      AS bllg_prov_npi,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_tax_id 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
    ELSE txn.payee_tax_id
    END                                                                   AS bllg_prov_tax_id,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_1_nm 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.payee_nm
    END                                                                   AS bllg_prov_1_nm,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_addr_1_txt 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.payee_addr_1
    END                                                                   AS bllg_prov_addr_1_txt,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_addr_2_txt 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.payee_addr_2
    END                                                                   AS bllg_prov_addr_2_txt,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_city_nm 
    --------------------------------------------------------------------------------------------------------------------------------            
     CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.payee_addr_city
    END                                                                   AS bllg_prov_city_nm,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_state_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    VALIDATE_STATE_CODE(UPPER(
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.payee_addr_state
    END))                                                                AS bllg_prov_state_cd,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_zip_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.payee_addr_zip
    END                                                                     AS bllg_prov_zip_cd,
    CAST(REPLACE(REPLACE(txn.tot_actual_provdr_paymt_amt, ",",""), "$","") AS FLOAT)  AS clm_prov_pymt_amt,
    CASE 
        WHEN txn.tot_actual_provdr_paymt_amt IS NOT NULL THEN  txn.txn_handling_cd
        ELSE NULL
    END                                                                     AS clm_prov_pymt_amt_qual,
    CAP_DATE(
            CAST(EXTRACT_DATE(txn.paymt_eff_dt, '%Y-%m-%d') AS DATE) ,
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}'                 AS DATE) 
              )                                                              AS clm_prov_pymt_dt,
    txn.claim_sts_cd			                                             AS clm_stat_cd,
    CAST(REPLACE(REPLACE(txn.tot_claim_charg_amt, ",",""), "$","") AS FLOAT) AS clm_submtd_chg_amt,
    CAST(REPLACE(REPLACE(txn.tot_paid_amt       , ",",""), "$","") AS FLOAT) AS clm_pymt_amt,
    CAST(REPLACE(REPLACE(txn.patnt_rspbty_amt   , ",",""), "$","") AS FLOAT) AS ptnt_respbty_amt,
    txn.payer_claim_flng_ind_cd	                                             AS medcl_covrg_typ_cd,
    txn.payer_claim_cntl_nbr	                                             AS payr_clm_ctl_num,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    pos_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    txn.pos_cd                                                              AS pos_cd,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    instnl_typ_of_bll_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    txn.instnl_typ_of_bll_cd                                                AS instnl_typ_of_bll_cd,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    drg_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    nullify_drg_blacklist(txn.drg_cd)                                         AS drg_cd,
    txn.drg_amt                                                               AS drg_weight_num,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_npi 
    --------------------------------------------------------------------------------------------------------------------------------            
    clean_up_npi_code(
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.rendr_provdr_npi
    END)                                                                      AS rndrg_prov_npi,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    bllg_prov_tax_id 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        ELSE txn.rendr_provdr_stlc_nbr
    END                                                                       AS rndrg_prov_state_lic_id, 
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    rndrg_prov_upin 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
    ELSE txn.rendr_provdr_upin
    END                                                                      AS rndrg_prov_upin,   
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    rndrg_prov_comrcl_id 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
    ELSE txn.rendr_provdr_comm_nbr
    END                                                                      AS rndrg_prov_comrcl_id,        
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------     
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.pos_cd  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')    THEN NULL 
        WHEN SUBSTR(txn.instnl_typ_of_bll_cd,  1, 1 )    = '3'                              THEN NULL  
        WHEN txn.rendr_provdr_last_nm IS NULL AND txn.rendr_provdr_first_nm IS NULL THEN NULL            
    ELSE TRIM(CONCAT(COALESCE(txn.rendr_provdr_last_nm,''), ' ', COALESCE(txn.rendr_provdr_first_nm, '')))
    END                                                                      AS rndrg_prov_1_nm,  
    --------------------------------------------------------------------------------------------------------------------------------        
    -------------------- claim adjustment section
    --------------------------------------------------------------------------------------------------------------------------------        
    txn.clm_adjmt_seq_num                                                    AS clm_adjmt_seq_num,
    txn.clm_adjmt_grp_cd                                                     AS clm_adjmt_grp_cd,
    txn.clm_adjmt_rsn_cd                                                     AS clm_adjmt_rsn_cd,
    txn.clm_adjmt_amt                                                        AS clm_adjmt_amt,
    txn.src_claim_id                                                         AS medcl_clm_lnk_txt,
    '216'                                                                    AS part_hvm_vdr_feed_id,
    CASE WHEN 0 = LENGTH(TRIM(COALESCE(
                    CAP_DATE(
                                CAST(COALESCE(txn.stmnt_from_dt, serviceLine.min_svc_from_dt, txn.stmnt_to_dt, serviceLine.max_svc_to_dt)   AS DATE) ,
                                CAST('{AVAILABLE_START_DATE}' AS DATE),
                                CAST('{VDR_FILE_DT}'                      AS DATE) 
                            )  
                        ,''))) 
         THEN  '0_PREDATES_HVM_HISTORY'
         ELSE CONCAT(SUBSTR(COALESCE(CAST(COALESCE(txn.stmnt_from_dt, serviceLine.min_svc_from_dt, txn.stmnt_to_dt, serviceLine.max_svc_to_dt, '{AVAILABLE_START_DATE}', '{VDR_FILE_DT}')   AS DATE)), 1, 4), '-',
                     SUBSTR(COALESCE(CAST(COALESCE(txn.stmnt_from_dt, serviceLine.min_svc_from_dt, txn.stmnt_to_dt, serviceLine.max_svc_to_dt, '{AVAILABLE_START_DATE}', '{VDR_FILE_DT}')   AS DATE)), 6, 2)) 
        END                                                                 AS part_mth


FROM practice_insight_claim_pos_tob_cas txn
LEFT OUTER JOIN (SELECT  txn_1.src_era_claim_id ,  
                     MAX(txn_1.svc_to_dt)   AS max_svc_to_dt,
                     MIN(txn_1.svc_from_dt) AS min_svc_from_dt
                   FROM practice_insight_claim_pos_tob_cas  txn_1 GROUP BY 1 ) serviceLine
   ON txn.src_era_claim_id = serviceLine.src_era_claim_id
