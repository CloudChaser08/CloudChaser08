SELECT
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    txn.src_era_claim_id,
    txn.src_claim_id,
    txn.claim_type_cd,
    EXTRACT_DATE(txn.edi_interchange_creation_dt          , '%Y-%m-%d') AS edi_interchange_creation_dt,
    txn.edi_interchange_creation_time,
    txn.txn_handling_cd,
    txn.tot_actual_provdr_paymt_amt,
    txn.credit_debit_fl_cd,
    txn.paymt_method_cd,
    txn.paymt_eff_dt,
    txn.payer_claim_cntl_nbr,
    txn.payer_claim_cntl_nbr_origl,
    txn.payer_nm,
    txn.payer_cms_plan_id,
    txn.payer_id,
    txn.payer_naic_id,
    txn.payer_group_nbr,
    txn.payer_claim_flng_ind_cd,
    txn.payee_tax_id,
    txn.payee_npi,
    txn.payee_nm,
    txn.payee_addr_1,
    txn.payee_addr_2,
    txn.payee_addr_city,
    txn.payee_addr_state,
    txn.payee_addr_zip,
    txn.rendr_provdr_npi,
    txn.rendr_provdr_upin,
    txn.rendr_provdr_comm_nbr,
    txn.rendr_provdr_loc_nbr,
    txn.rendr_provdr_stlc_nbr,
    txn.rendr_provdr_last_nm,
    txn.rendr_provdr_first_nm,
    txn.claim_sts_cd,
    txn.tot_claim_charg_amt,
    txn.tot_paid_amt,
    txn.patnt_rspbty_amt,
    txn.fclty_type_pos_cd,
    txn.claim_freq_cd,
    EXTRACT_DATE(txn.stmnt_from_dt          , '%Y-%m-%d') AS stmnt_from_dt,
    EXTRACT_DATE(txn.stmnt_to_dt            , '%Y-%m-%d') AS stmnt_to_dt,
    txn.covrg_expired_dt,
    EXTRACT_DATE(txn.claim_received_dt          , '%Y-%m-%d') AS claim_received_dt,
    txn.drg_cd,
    txn.drg_amt,
    --------------------------------------------------------------------------------------------------------------------------------        
    -------------------- claim adjustment section
    --------------------------------------------------------------------------------------------------------------------------------        
    POSEXPLODE(SPLIT(REPLACE(txn.clm_cas, '"', ''), ',')) AS (priority, value),    
    --------------------------------------------------------------------------------------------------------------------------------        
    txn.src_era_svc_id,
    txn.src_svc_id,
    txn.line_item_cntl_nbr,
    txn.svc_rendr_provdr_npi,
    txn.svc_rendr_provdr_upin,
    txn.svc_rendr_provdr_comm_nbr,
    txn.svc_rendr_provdr_loc_nbr,
    txn.svc_rendr_provdr_stlc_nbr,
    txn.adjtd_proc_cd_qual,
    txn.adjtd_proc_cd,
    txn.adjtd_proc_modfr_1,
    txn.adjtd_proc_modfr_2,
    txn.adjtd_proc_modfr_3,
    txn.adjtd_proc_modfr_4,
    txn.line_item_charg_amt,
    txn.line_item_allowed_amt,
    txn.line_item_paid_amt,
    txn.revnu_cd,
    txn.paid_units,
    txn.submd_proc_cd_qual,
    txn.submd_proc_cd,
    txn.submd_proc_modfr_1,
    txn.submd_proc_modfr_2,
    txn.submd_proc_modfr_3,
    txn.submd_proc_modfr_4,
    txn.origl_units_of_svc_cnt,
    EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'  ) AS svc_from_dt,
    EXTRACT_DATE(txn.svc_to_dt  , '%Y%m%d'  ) AS svc_to_dt,
    txn.svc_cas,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    pos_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.claim_type_cd = 'I' THEN NULL
        WHEN   
                LENGTH
                        (
                        TRIM
                            (
                            COALESCE
                                    (
                                        CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, ''))
                                    )
                            )
                        ) IN (1, 2)  
                AND RIGHT
                    (
                        CONCAT
                            ('00', CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')))
                        , 2
                    )   IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')        THEN '99'
                
        WHEN LENGTH
                        (
                            TRIM
                                (
                                    COALESCE
                                        (
                                            CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''
                                        )
                                )
                        ) IN (1, 2)                                                            THEN RIGHT(CONCAT('00',  TRIM(COALESCE(CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''))), 2) 
        ELSE NULL
    END                                                                         AS pos_cd,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    instnl_typ_of_bll_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.claim_type_cd = 'P'                                                              THEN NULL
        WHEN LENGTH
            (
                TRIM
                    (
                        COALESCE
                            (
                                CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''
                            )
                    )
            )  = 3  
        AND  SUBSTR
            (
                CONCAT
                    (
                        COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')
                    ), 1, 1
            )    <> '3'  
                                                                                             THEN   TRIM(COALESCE(CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''))
        WHEN LENGTH
            (
                TRIM
                    (
                        COALESCE
                            (
                                CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''
                            )
                    )
            )  = 3  
         AND  SUBSTR
             (
                 CONCAT
                     (
                         COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')
                     ), 1, 1
             )    = '3'                                                                       THEN   CONCAT('X', SUBSTR(TRIM(COALESCE(CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), '')), 2, 10))
        
        ELSE NULL
    END                                                                         AS instnl_typ_of_bll_cd
    
    -------------------

FROM  txn 
WHERE txn.clm_cas IS NOT NULL
UNION ALL
SELECT
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    txn.src_era_claim_id,
    txn.src_claim_id,
    txn.claim_type_cd,
    EXTRACT_DATE(txn.edi_interchange_creation_dt          , '%Y-%m-%d') AS edi_interchange_creation_dt,
    txn.edi_interchange_creation_time,
    txn.txn_handling_cd,
    txn.tot_actual_provdr_paymt_amt,
    txn.credit_debit_fl_cd,
    txn.paymt_method_cd,
    txn.paymt_eff_dt,
    txn.payer_claim_cntl_nbr,
    txn.payer_claim_cntl_nbr_origl,
    txn.payer_nm,
    txn.payer_cms_plan_id,
    txn.payer_id,
    txn.payer_naic_id,
    txn.payer_group_nbr,
    txn.payer_claim_flng_ind_cd,
    txn.payee_tax_id,
    txn.payee_npi,
    txn.payee_nm,
    txn.payee_addr_1,
    txn.payee_addr_2,
    txn.payee_addr_city,
    txn.payee_addr_state,
    txn.payee_addr_zip,
    txn.rendr_provdr_npi,
    txn.rendr_provdr_upin,
    txn.rendr_provdr_comm_nbr,
    txn.rendr_provdr_loc_nbr,
    txn.rendr_provdr_stlc_nbr,
    txn.rendr_provdr_last_nm,
    txn.rendr_provdr_first_nm,
    txn.claim_sts_cd,
    txn.tot_claim_charg_amt,
    txn.tot_paid_amt,
    txn.patnt_rspbty_amt,
    txn.fclty_type_pos_cd,
    txn.claim_freq_cd,
    EXTRACT_DATE(txn.stmnt_from_dt          , '%Y-%m-%d') AS stmnt_from_dt,
    EXTRACT_DATE(txn.stmnt_to_dt            , '%Y-%m-%d') AS stmnt_to_dt,
    txn.covrg_expired_dt,
    EXTRACT_DATE(txn.claim_received_dt          , '%Y-%m-%d') AS claim_received_dt,
    txn.drg_cd,
    txn.drg_amt,
    --------------------------------------------------------------------------------------------------------------------------------        
    -------------------- claim adjustment section
    --------------------------------------------------------------------------------------------------------------------------------
    CAST( NULL  AS STRING) AS priority,
    CAST( NULL  AS STRING) AS value,

    --------------------------------------------------------------------------------------------------------------------------------        
    txn.src_era_svc_id,
    txn.src_svc_id,
    txn.line_item_cntl_nbr,
    txn.svc_rendr_provdr_npi,
    txn.svc_rendr_provdr_upin,
    txn.svc_rendr_provdr_comm_nbr,
    txn.svc_rendr_provdr_loc_nbr,
    txn.svc_rendr_provdr_stlc_nbr,
    txn.adjtd_proc_cd_qual,
    txn.adjtd_proc_cd,
    txn.adjtd_proc_modfr_1,
    txn.adjtd_proc_modfr_2,
    txn.adjtd_proc_modfr_3,
    txn.adjtd_proc_modfr_4,
    txn.line_item_charg_amt,
    txn.line_item_allowed_amt,
    txn.line_item_paid_amt,
    txn.revnu_cd,
    txn.paid_units,
    txn.submd_proc_cd_qual,
    txn.submd_proc_cd,
    txn.submd_proc_modfr_1,
    txn.submd_proc_modfr_2,
    txn.submd_proc_modfr_3,
    txn.submd_proc_modfr_4,
    txn.origl_units_of_svc_cnt,
    EXTRACT_DATE(txn.svc_from_dt, '%Y%m%d'  ) AS svc_from_dt,
    EXTRACT_DATE(txn.svc_to_dt  , '%Y%m%d'  ) AS svc_to_dt,
    txn.svc_cas,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    pos_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.claim_type_cd = 'I' THEN NULL
        WHEN   
                LENGTH
                        (
                        TRIM
                            (
                            COALESCE
                                    (
                                        CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, ''))
                                    )
                            )
                        ) IN (1, 2)  
                AND RIGHT
                    (
                        CONCAT
                            ('00', CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')))
                        , 2
                    )   IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')        THEN '99'
                
        WHEN LENGTH
                        (
                            TRIM
                                (
                                    COALESCE
                                        (
                                            CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''
                                        )
                                )
                        ) IN (1, 2)                                                            THEN RIGHT(CONCAT('00',  TRIM(COALESCE(CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''))), 2) 
        ELSE NULL
    END                                                                         AS pos_cd,
    --------------------------------------------------------------------------------------------------------------------------------        
    ------------------------------    instnl_typ_of_bll_cd 
    --------------------------------------------------------------------------------------------------------------------------------            
    CASE 
        WHEN txn.claim_type_cd = 'P'                                                              THEN NULL
        WHEN LENGTH
            (
                TRIM
                    (
                        COALESCE
                            (
                                CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''
                            )
                    )
            )  = 3  
        AND  SUBSTR
            (
                CONCAT
                    (
                        COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')
                    ), 1, 1
            )    <> '3'  
                                                                                             THEN   TRIM(COALESCE(CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''))
        WHEN LENGTH
            (
                TRIM
                    (
                        COALESCE
                            (
                                CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), ''
                            )
                    )
            )  = 3  
         AND  SUBSTR
             (
                 CONCAT
                     (
                         COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')
                     ), 1, 1
             )    = '3'                                                                       THEN   CONCAT('X', SUBSTR(TRIM(COALESCE(CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, '')), '')), 2, 10))
        
        ELSE NULL
    END                                                                         AS instnl_typ_of_bll_cd
    -------------------

FROM txn 
WHERE txn.clm_cas IS NULL
