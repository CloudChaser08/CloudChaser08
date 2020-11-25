SELECT
MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
  CONCAT('83_', header.batch_id)                                                                    AS hv_medcl_clm_pymt_dtl_id,
  CURRENT_DATE                                                                                      AS crt_dt,
  '03'                                                                                              AS mdl_vrsn_num,
  35                                                                                                AS hvm_vdr_id,
  83                                                                                                AS hvm_vdr_feed_id,
  header.Batch_ID                                                                                   AS vdr_medcl_clm_pymt_sumry_id,
  CASE WHEN header.Batch_ID IS NOT NULL
       THEN 'VENDOR' 
       ELSE NULL END                                                                                AS vdr_medcl_clm_pymt_sumry_id_qual,
  serviceline.Claim_ID                                                                              AS vdr_medcl_clm_pymt_dtl_id,
  CASE WHEN serviceline.Claim_ID IS NOT NULL
       THEN 'VENDOR' 
       ELSE NULL END                                                                                AS vdr_medcl_clm_pymt_dtl_id_qual,  
  CAP_DATE(
        CAST(EXTRACT_DATE(
                         COALESCE(serviceline.Service_From_Date,claim.Claim_Statement_Period_Start)
                        ,'%Y%m%d'
                         ) AS DATE 
            ),
                 esdt.gen_ref_1_dt,
                 CAST('{VDR_FILE_DT}' AS DATE)
            )                                                                                       AS svc_ln_start_dt,
  CAP_DATE(
        CAST(EXTRACT_DATE(
                           COALESCE(serviceline.Service_to_Date,claim.Claim_Statement_Period_End)
                               ,'%Y%m%d'
                         ) AS DATE
                ),
                 esdt.gen_ref_1_dt,
                 CAST('{VDR_FILE_DT}' AS DATE)
              )                                                                                     AS svc_ln_end_dt,
  CASE WHEN claim.clmpvt_pos_cd = '99' OR SUBSTR(claim.clmpvt_pos_cd_instnl_typ_of_bll_cd, 1, 1) = 'X' THEN NULL 
       WHEN serviceline.Rendering_Provider_ID_Qual_1_ = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__1, ''))) THEN serviceline.Rendering_Provider_ID__1 
       WHEN serviceline.Rendering_Provider_ID_Qual_2  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_2, ''))) THEN serviceline.Rendering_Provider_ID_2 
       WHEN serviceline.Rendering_Provider_ID_Qual_3  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__3, ''))) THEN serviceline.Rendering_Provider_ID__3 
       WHEN serviceline.Rendering_Provider_ID_Qual_4  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__4, ''))) THEN serviceline.Rendering_Provider_ID__4 
       WHEN serviceline.Rendering_Provider_ID_Qual_5  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__5, ''))) THEN serviceline.Rendering_Provider_ID__5 
       WHEN serviceline.Rendering_Provider_ID_Qual_6  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__6, ''))) THEN serviceline.Rendering_Provider_ID__6 
       WHEN serviceline.Rendering_Provider_ID_Qual_7  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__7, ''))) THEN serviceline.Rendering_Provider_ID__7 
       WHEN serviceline.Rendering_Provider_ID_Qual_8  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__8, ''))) THEN serviceline.Rendering_Provider_ID__8 
       WHEN serviceline.Rendering_Provider_ID_Qual_9  = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__9, ''))) THEN serviceline.Rendering_Provider_ID__9 
       WHEN serviceline.Rendering_Provider_ID_Qual_10 = 'HPI' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_10, ''))) THEN serviceline.Rendering_Provider_ID_10 
       ELSE NULL END                                                                                AS rndrg_prov_npi,
  CASE WHEN claim.clmpvt_pos_cd = '99' OR SUBSTR(claim.clmpvt_pos_cd_instnl_typ_of_bll_cd, 1, 1) = 'X' THEN NULL 
       WHEN serviceline.Rendering_Provider_ID_Qual_1_ = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__1, ''))) THEN serviceline.Rendering_Provider_ID__1 
       WHEN serviceline.Rendering_Provider_ID_Qual_2  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_2, ''))) THEN serviceline.Rendering_Provider_ID_2 
       WHEN serviceline.Rendering_Provider_ID_Qual_3  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__3, ''))) THEN serviceline.Rendering_Provider_ID__3 
       WHEN serviceline.Rendering_Provider_ID_Qual_4  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__4, ''))) THEN serviceline.Rendering_Provider_ID__4 
       WHEN serviceline.Rendering_Provider_ID_Qual_5  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__5, ''))) THEN serviceline.Rendering_Provider_ID__5 
       WHEN serviceline.Rendering_Provider_ID_Qual_6  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__6, ''))) THEN serviceline.Rendering_Provider_ID__6 
       WHEN serviceline.Rendering_Provider_ID_Qual_7  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__7, ''))) THEN serviceline.Rendering_Provider_ID__7 
       WHEN serviceline.Rendering_Provider_ID_Qual_8  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__8, ''))) THEN serviceline.Rendering_Provider_ID__8 
       WHEN serviceline.Rendering_Provider_ID_Qual_9  = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__9, ''))) THEN serviceline.Rendering_Provider_ID__9 
       WHEN serviceline.Rendering_Provider_ID_Qual_10 = 'TJ' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_10, ''))) THEN serviceline.Rendering_Provider_ID_10 
       ELSE NULL END                                                                               AS rndrg_prov_tax_id,
  CASE WHEN claim.clmpvt_pos_cd = '99' OR SUBSTR(claim.clmpvt_pos_cd_instnl_typ_of_bll_cd, 1, 1) = 'X' THEN NULL 
       WHEN serviceline.Rendering_Provider_ID_Qual_1_ = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__1, ''))) THEN serviceline.Rendering_Provider_ID__1 
       WHEN serviceline.Rendering_Provider_ID_Qual_2  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_2, ''))) THEN serviceline.Rendering_Provider_ID_2 
       WHEN serviceline.Rendering_Provider_ID_Qual_3  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__3, ''))) THEN serviceline.Rendering_Provider_ID__3 
       WHEN serviceline.Rendering_Provider_ID_Qual_4  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__4, ''))) THEN serviceline.Rendering_Provider_ID__4 
       WHEN serviceline.Rendering_Provider_ID_Qual_5  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__5, ''))) THEN serviceline.Rendering_Provider_ID__5 
       WHEN serviceline.Rendering_Provider_ID_Qual_6  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__6, ''))) THEN serviceline.Rendering_Provider_ID__6 
       WHEN serviceline.Rendering_Provider_ID_Qual_7  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__7, ''))) THEN serviceline.Rendering_Provider_ID__7 
       WHEN serviceline.Rendering_Provider_ID_Qual_8  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__8, ''))) THEN serviceline.Rendering_Provider_ID__8 
       WHEN serviceline.Rendering_Provider_ID_Qual_9  = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__9, ''))) THEN serviceline.Rendering_Provider_ID__9 
       WHEN serviceline.Rendering_Provider_ID_Qual_10 = 'SY' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_10, ''))) THEN serviceline.Rendering_Provider_ID_10 
       ELSE NULL END                                                                               AS rndrg_prov_ssn,     
  CASE WHEN claim.clmpvt_pos_cd = '99' OR SUBSTR(claim.clmpvt_pos_cd_instnl_typ_of_bll_cd, 1, 1) = 'X' THEN NULL 
       WHEN serviceline.Rendering_Provider_ID_Qual_1_ = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__1, ''))) THEN serviceline.Rendering_Provider_ID__1 
       WHEN serviceline.Rendering_Provider_ID_Qual_2  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_2, ''))) THEN serviceline.Rendering_Provider_ID_2 
       WHEN serviceline.Rendering_Provider_ID_Qual_3  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__3, ''))) THEN serviceline.Rendering_Provider_ID__3 
       WHEN serviceline.Rendering_Provider_ID_Qual_4  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__4, ''))) THEN serviceline.Rendering_Provider_ID__4 
       WHEN serviceline.Rendering_Provider_ID_Qual_5  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__5, ''))) THEN serviceline.Rendering_Provider_ID__5 
       WHEN serviceline.Rendering_Provider_ID_Qual_6  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__6, ''))) THEN serviceline.Rendering_Provider_ID__6 
       WHEN serviceline.Rendering_Provider_ID_Qual_7  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__7, ''))) THEN serviceline.Rendering_Provider_ID__7 
       WHEN serviceline.Rendering_Provider_ID_Qual_8  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__8, ''))) THEN serviceline.Rendering_Provider_ID__8 
       WHEN serviceline.Rendering_Provider_ID_Qual_9  = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__9, ''))) THEN serviceline.Rendering_Provider_ID__9 
       WHEN serviceline.Rendering_Provider_ID_Qual_10 = '0B' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_10, ''))) THEN serviceline.Rendering_Provider_ID_10 
       ELSE NULL END                                                                               AS rndrg_prov_state_lic_id, 
  CASE WHEN claim.clmpvt_pos_cd = '99' OR SUBSTR(claim.clmpvt_pos_cd_instnl_typ_of_bll_cd, 1, 1) = 'X' THEN NULL 
       WHEN serviceline.Rendering_Provider_ID_Qual_1_ = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__1, ''))) THEN serviceline.Rendering_Provider_ID__1 
       WHEN serviceline.Rendering_Provider_ID_Qual_2  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_2, ''))) THEN serviceline.Rendering_Provider_ID_2 
       WHEN serviceline.Rendering_Provider_ID_Qual_3  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__3, ''))) THEN serviceline.Rendering_Provider_ID__3 
       WHEN serviceline.Rendering_Provider_ID_Qual_4  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__4, ''))) THEN serviceline.Rendering_Provider_ID__4 
       WHEN serviceline.Rendering_Provider_ID_Qual_5  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__5, ''))) THEN serviceline.Rendering_Provider_ID__5 
       WHEN serviceline.Rendering_Provider_ID_Qual_6  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__6, ''))) THEN serviceline.Rendering_Provider_ID__6 
       WHEN serviceline.Rendering_Provider_ID_Qual_7  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__7, ''))) THEN serviceline.Rendering_Provider_ID__7 
       WHEN serviceline.Rendering_Provider_ID_Qual_8  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__8, ''))) THEN serviceline.Rendering_Provider_ID__8 
       WHEN serviceline.Rendering_Provider_ID_Qual_9  = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__9, ''))) THEN serviceline.Rendering_Provider_ID__9 
       WHEN serviceline.Rendering_Provider_ID_Qual_10 = '1G' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_10, ''))) THEN serviceline.Rendering_Provider_ID_10 
       ELSE NULL END                                                                               AS rndrg_prov_upin, 
  CASE WHEN claim.clmpvt_pos_cd = '99' OR SUBSTR(claim.clmpvt_pos_cd_instnl_typ_of_bll_cd, 1, 1) = 'X' THEN NULL 
       WHEN serviceline.Rendering_Provider_ID_Qual_1_ = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__1, ''))) THEN serviceline.Rendering_Provider_ID__1 
       WHEN serviceline.Rendering_Provider_ID_Qual_2  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_2, ''))) THEN serviceline.Rendering_Provider_ID_2 
       WHEN serviceline.Rendering_Provider_ID_Qual_3  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__3, ''))) THEN serviceline.Rendering_Provider_ID__3 
       WHEN serviceline.Rendering_Provider_ID_Qual_4  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__4, ''))) THEN serviceline.Rendering_Provider_ID__4 
       WHEN serviceline.Rendering_Provider_ID_Qual_5  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__5, ''))) THEN serviceline.Rendering_Provider_ID__5 
       WHEN serviceline.Rendering_Provider_ID_Qual_6  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__6, ''))) THEN serviceline.Rendering_Provider_ID__6 
       WHEN serviceline.Rendering_Provider_ID_Qual_7  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__7, ''))) THEN serviceline.Rendering_Provider_ID__7 
       WHEN serviceline.Rendering_Provider_ID_Qual_8  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__8, ''))) THEN serviceline.Rendering_Provider_ID__8 
       WHEN serviceline.Rendering_Provider_ID_Qual_9  = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID__9, ''))) THEN serviceline.Rendering_Provider_ID__9 
       WHEN serviceline.Rendering_Provider_ID_Qual_10 = 'G2' AND 0 <> LENGTH(TRIM(COALESCE(serviceline.Rendering_Provider_ID_10, ''))) THEN serviceline.Rendering_Provider_ID_10 
       ELSE NULL END                                                                               AS rndrg_prov_comrcl_id,       
  CASE WHEN serviceline.Product_or_Service_ID_Qualifier IN ('NU','N4') THEN NULL
       ELSE CLEAN_UP_PROCEDURE_CODE(serviceline.Adjudicated_Procedure_Code)     
        END                                                                                        AS adjctd_proc_cd,
  CASE WHEN serviceline.Product_or_Service_ID_Qualifier IN ('NU','N4') THEN NULL
       WHEN 0 <> LENGTH(TRIM(COALESCE(CLEAN_UP_PROCEDURE_CODE(serviceline.Adjudicated_Procedure_Code),''))) THEN serviceline.Product_or_Service_ID_Qualifier
       ELSE NULL END                                                                               AS adjctd_proc_cd_qual,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Adjudicated_Procedure_Modifier_1),1,2)             AS adjctd_proc_cd_1_modfr,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Adjudicated_Procedure_Modifier_2),1,2)             AS adjctd_proc_cd_2_modfr,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Adjudicated_Procedure_Modifer_3),1,2)              AS adjctd_proc_cd_3_modfr,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Adjudicated_Procedure_Modifier_4),1,2)             AS adjctd_proc_cd_4_modfr,  
  CASE WHEN serviceline.Original_Product_or_Service_ID_Qualifier IN ('NU','N4') THEN NULL
       ELSE CLEAN_UP_PROCEDURE_CODE(serviceline.Original_Submitted_Procedure_Code)     
       END                                                                                         AS orig_submtd_proc_cd,
  CASE WHEN serviceline.Original_Product_or_Service_ID_Qualifier IN ('NU','N4') THEN NULL
       WHEN 0 <> LENGTH(TRIM(COALESCE(CLEAN_UP_PROCEDURE_CODE(serviceline.Original_Submitted_Procedure_Code),''))) THEN serviceline.Original_Product_or_Service_ID_Qualifier
       ELSE NULL END                                                                               AS orig_submtd_proc_cd_qual,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Original_Submitted_Procedure_Modifiers_1),1,2)     AS orig_submtd_proc_cd_1_modfr,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Original_Submitted_Procedure_Modifiers_2),1,2)     AS orig_submtd_proc_cd_2_modfr,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Original_Submitted_Procedure_Modifiers_3),1,2)     AS orig_submtd_proc_cd_3_modfr,
  SUBSTRING(CLEAN_UP_PROCEDURE_CODE(serviceline.Original_Submitted_Procedure_Modifiers_4),1,2)     AS orig_submtd_proc_cd_4_modfr,
  serviceline.Line_Item_Charge_Amount                                                              AS svc_ln_submtd_chg_amt,
  serviceline.Line_Item_Provider_Payment_Amount                                                    AS svc_ln_prov_pymt_amt,
  serviceline.NUBC_Revenue_Code                                                                    AS rev_cd,
  serviceline.Units_of_Service_Paid_Count                                                          AS paid_svc_unt_cnt,
  serviceline.Original_Units_of_Service_Count                                                      AS orig_svc_unt_cnt,
  serviceline.svcpvt_svc_ln_adjmt_grp_cd                                                           AS svc_ln_adjmt_grp_cd,
  serviceline.svcpvt_svc_ln_adjmt_seq_num                                                          AS svc_ln_adjmt_seq_num,
  serviceline.svcpvt_svc_ln_adjmt_rsn_txt                                                          AS svc_ln_adjmt_rsn_cd,
  serviceline.svcpvt_svc_ln_adjmt_amt                                                              AS svc_ln_adjmt_amt,
  serviceline.svcpvt_svc_ln_adjmt_qty                                                              AS svc_ln_adjmt_qty,
  serviceline.svcpvt_svc_ln_suplmtl_amt                                                            AS svc_ln_suplmtl_amt,
  serviceline.svcpvt_svc_ln_suplmtl_amt_qual                                                       AS svc_ln_suplmtl_amt_qual,
  serviceline.svcpvt_svc_ln_rmrk_cd_seq_num                                                        AS svc_ln_rmrk_cd_seq_num,
  serviceline.svcpvt_svc_ln_remrk_cd                                                               AS svc_ln_remrk_cd,
  serviceline.svcpvt_svc_ln_remrk_cd_qual                                                          AS svc_ln_remrk_cd_qual,  
  '83'                                                                                             AS part_hvm_vdr_feed_id,
 CASE WHEN 0 = LENGTH(TRIM(COALESCE(
                            CAP_DATE(
                            COALESCE(CAST(EXTRACT_DATE(serviceline.Service_From_Date, '%Y%m%d') AS DATE),  CAST('{VDR_FILE_DT}' AS DATE)),   -- Replace w/vendor's file date.
                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt,CAST('1901-01-01' as date)),
                             CAST('{VDR_FILE_DT}' AS DATE) )  -- Replace w/vendor's file date.
                        ,''))) 
      THEN  '0_PREDATES_HVM_HISTORY'
      ELSE CONCAT(SUBSTR(COALESCE(CAST(EXTRACT_DATE(serviceline.Service_From_Date, '%Y%m%d') AS DATE),  CAST('{VDR_FILE_DT}' AS DATE)), 1, 4), '-',
                     SUBSTR(COALESCE(CAST(EXTRACT_DATE(serviceline.Service_From_Date, '%Y%m%d') AS DATE),  CAST('{VDR_FILE_DT}' AS DATE)), 6, 2)) 
      END                                                                                          AS part_mth 
FROM era_header header
LEFT OUTER JOIN veradigm_era_serviceline_pivot_final serviceline ON header.Batch_ID = serviceline.Batch_ID  
LEFT OUTER JOIN (SELECT clm.Batch_ID AS Batch_ID, clm.Payer_Claim_Control_Number AS Payer_Claim_Control_Number, 
                        MAX(clm.Claim_Statement_Period_Start) AS Claim_Statement_Period_Start,
                        MIN(clm.Claim_Statement_Period_End) AS Claim_Statement_Period_End,
                        MAX(clm.clmpvt_pos_cd) AS clmpvt_pos_cd,
                        MAX(clm.clmpvt_pos_cd_instnl_typ_of_bll_cd) AS clmpvt_pos_cd_instnl_typ_of_bll_cd
                   FROM veradigm_era_claimpayment_pivot_final clm
                   INNER JOIN era_header hdr ON  hdr.Batch_ID = clm.Batch_ID GROUP BY 1,2) claim
   ON serviceline.Batch_ID = claim.Batch_ID 
  AND serviceline.claim_id = claim.Payer_Claim_Control_Number
 LEFT OUTER JOIN ref_gen_ref esdt
   ON 1 = 1
  AND esdt.hvm_vdr_feed_id = 83
  AND esdt.gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
 LEFT OUTER JOIN ref_gen_ref ahdt
   ON 1 = 1
  AND ahdt.hvm_vdr_feed_id = 83
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE' 
WHERE header.Header_Indicator='HDR' 
-- AND header.batch_id  IN ('201707281308100400000004000879')
