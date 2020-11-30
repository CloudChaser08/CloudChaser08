SELECT
MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
  CONCAT('83_', header.batch_id)                                            AS hv_medcl_clm_pymt_sumry_id,
  CURRENT_DATE                                                              AS crt_dt,
  '03'                                                                      AS mdl_vrsn_num,
  35                                                                        AS hvm_vdr_id,
  83                                                                        AS hvm_vdr_feed_id,
  header.Batch_ID                                                           AS vdr_medcl_clm_pymt_sumry_id,
  CASE WHEN header.Batch_ID IS NOT NULL
       THEN 'VENDOR' 
       ELSE NULL END                                                        AS vdr_medcl_clm_pymt_sumry_id_qual,
  CAP_DATE(
        CAST(EXTRACT_DATE(
                         COALESCE(
                            claimpayment.Claim_Statement_Period_Start,
                            serviceLine.Service_From_Date
                                 )
                     ,'%Y%m%d'
                        ) AS DATE 
            ),
                 esdt.gen_ref_1_dt,
                  CAST('{VDR_FILE_DT}' AS DATE)
            )                                                               AS clm_stmt_perd_start_dt,
  CAP_DATE(
        CAST(EXTRACT_DATE(
                           COALESCE(
                               claimpayment.Claim_Statement_Period_End,
                               serviceLine.Service_to_Date
                                    )
                               ,'%Y%m%d'
                        ) AS DATE
                ),
                 esdt.gen_ref_1_dt,
                  CAST('{VDR_FILE_DT}' AS DATE)
              )                                                            AS clm_stmt_perd_end_dt,
  CAP_DATE(
        CAST(EXTRACT_DATE(
                           claimpayment.Claim_Received_Date,'%Y%m%d'
                          ) AS DATE
            ),
             esdt.gen_ref_1_dt,
              CAST('{VDR_FILE_DT}' AS DATE)
              )                                                             AS payr_clm_recpt_dt,
  header.TSPID                                                              AS payr_id,
  (CASE WHEN  0 <> LENGTH(TRIM(COALESCE(header.TSPID, '')))
        THEN 'TSPID'
        ELSE NULL
   END)                                                                     AS payr_id_qual,
  header.Payer_Name                                                         AS payr_nm,
 
   (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
         THEN NULL 
         ELSE header.Payee_NPI  
    END)                                                                    AS bllg_prov_npi,
   (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
         THEN NULL 
         ELSE header.Payee_Addtl_Identification 
    END)                                                                    AS bllg_prov_vdr_id,
    (CASE WHEN  0 <> LENGTH(TRIM(COALESCE(header.Payee_Addtl_Identification, '')))
          THEN  'VENDOR'
          ELSE NULL
    END)                                                                    AS bllg_prov_vdr_id_qual,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
          THEN NULL 
          ELSE header.Payee_Tax_ID
    END) AS bllg_prov_tax_id,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
          THEN NULL 
          ELSE header.State_License_Number
    END)                                                                    AS bllg_prov_state_lic_id,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
          THEN NULL 
          ELSE header.National_Association_of_Boards_of_Pharmacy_Number
     END)                                                                   AS bllg_prov_nabp_id,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE header.payee_name
     END)                                                                   AS bllg_prov_1_nm,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE header.Payee_Address_Line 
     END)                                                                   AS bllg_prov_addr_1_txt,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
          THEN NULL 
          ELSE header.Payee_Address_Line_2
     END)                                                                   AS bllg_prov_addr_2_txt,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE header.Payee_City_Name
     END)                                                                   AS bllg_prov_city_nm,
     VALIDATE_STATE_CODE(UPPER(COALESCE(
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE header.Payee_State_Code
     END),'')))                                                             AS bllg_prov_state_cd,
    (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL
        ELSE header.Payee_Postal_Zone_or_ZIP_Code
     END)                                                                   AS bllg_prov_zip_cd,
  claimpayment.Claim_Status_Code                                            AS clm_stat_cd,
  claimpayment.Total_Claim_Charge_Amount                                    AS clm_submtd_chg_amt,
  claimpayment.Claim_Payment_Amount                                         AS clm_pymt_amt,
  claimpayment.Patient_Responsibility_Amount                                AS ptnt_respbty_amt,
  claimpayment.Claim_Filing_Indicator_Code                                  AS medcl_covrg_typ_cd,
  MD5(claimpayment.Payer_Claim_Control_Number)                              AS payr_clm_ctl_num,
  claimpayment.clmpvt_pos_cd                                                AS pos_cd,
  claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd                           AS instnl_typ_of_bll_cd,
  NULLIFY_DRG_BLACKLIST(claimpayment.DRG_Code)                              AS drg_cd,
  claimpayment.DRG_Weight                                                   AS drg_weight_num,
  claimpayment.Discharge_Fraction                                           AS dischg_frctn_num,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.Rendering_Provider_NPI 
    END)                                                                    AS rndrg_prov_npi,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.Rendering_Provider_Identifier 
    END)                                                                    AS rndrg_prov_vdr_id,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.State_License_Number 
    END)                                                                    AS rndrg_prov_state_lic_id,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.Provider_UPIN 
    END)                                                                    AS rndrg_prov_upin,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.Provider_Commerical_Number 
    END)                                                                    AS rndrg_prov_comrcl_id,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.Rendering_Provider_Last_or_Organization_Name 
    END)                                                                    AS rndrg_prov_1_nm,
  (CASE WHEN claimpayment.clmpvt_pos_cd  = '99' 
              OR SUBSTR(claimpayment.clmpvt_pos_cd_instnl_typ_of_bll_cd , 1, 1) = 'X' 
        THEN NULL 
        ELSE claimpayment.Rendering_Provider_First_Name 
    END)                                                                    AS rndrg_prov_2_nm,
  claimpayment.Coordination_of_Benefits_Carrier_Name                        AS cob_pyr_nm,
  claimpayment.Coordination_of_Benefits_Carrier_Identifier                  AS cob_pyr_id,
  (CASE WHEN  0 <> LENGTH(TRIM(COALESCE(claimpayment.Coordination_of_Benefits_Carrier_Identifier, '')))
       THEN  claimpayment.Identification_Code_Qualifier_2
       ELSE NULL
   END)                                                                     AS cob_pyr_id_qual,
  claimpayment.Covered_Days_or_Visits_Count                                 AS covrd_day_vst_cnt,
  claimpayment.PPS_Operaqting_Outlier_Amt                                   AS pps_operg_outlr_amt,
  claimpayment.Lifetime_Psychiatric_Days_Count                              AS lftm_psychtrc_day_cnt,
  claimpayment.Claim_DRG_Amt                                                AS clm_drg_amt,
  claimpayment.Claim_Disproportionate_Share_Amt                             AS clm_dsh_amt,
  claimpayment.Claim_MSP_Passthrough_Amt                                    AS clm_msp_pass_thru_amt,
  claimpayment.Claim_PPS_Capital_Amt                                        AS clm_pps_captl_amt,
  claimpayment.PPS_Capital_FSP_DRG_Amt_                                     AS pps_captl_fsp_drg_amt,
  claimpayment.PPS_Capital_HSP_DRG_Amt                                      AS pps_captl_hsp_drg_amt,
  claimpayment.PPS_Capital_DSH_DRG_Amt                                      AS pps_captl_dsh_drg_amt,
  claimpayment.Old_Capital_Amt                                              AS prev_rptg_perd_captl_amt,
  claimpayment.PPS_Capital_IME_Amt                                          AS pps_captl_ime_drg_amt,
  claimpayment.PPS_Operating_Hospital_Specific_DRG_Amt                      AS pps_operg_hsp_drg_amt,
  claimpayment.Cost_Report_Day_Count                                        AS cost_rpt_day_cnt,
  claimpayment.PPS_Operating_Federal_Specific_DRG_Amt                       AS pps_operg_fsp_drg_amt,
  claimpayment.Claim_PPS_Capital_Outlier_Amt                                AS clm_pps_captl_outlr_amt,
  claimpayment.Claim_Indirect_Teaching_Amt                                  AS clm_indrct_tchg_amt,
  claimpayment.Nonpayable_Professional_Component_Amt                        AS non_paybl_profnl_compnt_amt,
  claimpayment.clmpvt_clm_pymt_remrk_cd_seq_num                             AS clm_pymt_remrk_cd_seq_num,
  claimpayment.clmpvt_clm_pymt_remrk_cd                                     AS clm_pymt_remrk_cd, 
  claimpayment.PPS_Capital_Exception_Amt                                    AS pps_captl_excptn_amt,
  claimpayment.Reimbursement_Rate                                           AS reimbmt_rate_pct,
  claimpayment.Claim_HCPCS_Payable_Amount                                   AS clm_hcpcs_paybl_amt,
  claimpayment.Claim_ESRD_Payment_Amount                                    AS clm_esrd_pymt_amt,
  claimpayment.clmpvt_clm_amt_seq_num                                       AS clm_amt_seq_num,
  claimpayment.clmpvt_clm_amt                                               AS clm_amt,
  claimpayment.clmpvt_clm_amt_qual                                          AS clm_amt_qual,
  claimpayment.clmpvt_clm_qty_seq_num                                       AS clm_qty_seq_num,    
  claimpayment.clmpvt_clm_qty                                               AS clm_qty,
  claimpayment.clmpvt_clm_qty_qual                                          AS clm_qty_qual,
  claimpayment.clmpvt_clm_adjmt_seq_num                                     AS clm_adjmt_seq_num,
  claimpayment.clmpvt_clm_adjmt_grp_cd                                      AS clm_adjmt_grp_cd,
  claimpayment.clmpvt_clm_adjmt_rsn_txt                                      AS clm_adjmt_rsn_cd,
  claimpayment.clmpvt_clm_adjmt_amt                                         AS clm_adjmt_amt,
  claimpayment.clmpvt_clm_adjmt_qty                                         AS clm_adjmt_qty,
  payload.pcn                                                               AS medcl_clm_lnk_txt,
   '83'                                                                     AS part_hvm_vdr_feed_id,
    CASE WHEN 0 = LENGTH(TRIM(COALESCE(
                            CAP_DATE(
                            COALESCE(CAST(EXTRACT_DATE(claimpayment.Claim_Statement_Period_End, '%Y%m%d') AS DATE),  CAST('{VDR_FILE_DT}' AS DATE)),   -- Replace w/vendor's file date.
                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt,CAST('1901-01-01' as date)),
                             CAST('{VDR_FILE_DT}' AS DATE) )  -- Replace w/vendor's file date.
                        ,''))) 
         THEN  '0_PREDATES_HVM_HISTORY'
         ELSE CONCAT(SUBSTR(COALESCE(CAST(EXTRACT_DATE(claimpayment.Claim_Statement_Period_End, '%Y%m%d') AS DATE),  CAST('{VDR_FILE_DT}' AS DATE)), 1, 4), '-',
                     SUBSTR(COALESCE(CAST(EXTRACT_DATE(claimpayment.Claim_Statement_Period_End, '%Y%m%d') AS DATE),  CAST('{VDR_FILE_DT}' AS DATE)), 6, 2)) 
        END                                                                 AS part_mth
FROM era_header header 
LEFT OUTER JOIN matching_payload payload                                          ON header.Batch_ID = payload.claimid 
LEFT OUTER JOIN veradigm_era_claimpayment_pivot_final claimpayment     ON payload.hvJoinKey = claimpayment.hvJoinKey
LEFT OUTER JOIN (SELECT sl.Batch_ID AS Batch_ID, sl.claim_id, MAX(sl.Service_to_Date) AS Service_to_Date,
                        MIN(sl.Service_From_Date) AS Service_From_Date 
                   FROM era_service_line sl 
                     INNER JOIN era_header hdr ON  hdr.Batch_ID = sl.Batch_ID GROUP BY 1,2) serviceLine
   ON claimpayment.Batch_ID = serviceLine.Batch_ID 
  AND claimpayment.Payer_Claim_Control_Number = serviceLine.claim_id
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
