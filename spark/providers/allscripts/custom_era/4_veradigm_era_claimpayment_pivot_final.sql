SELECT
    claimpayment.Batch_ID,
    claimpayment.hvJoinKey,
    claimpayment.Claim_Statement_Period_Start,
    claimpayment.Claim_Statement_Period_End,
    claimpayment.Claim_Received_Date,
    claimpayment.Claim_Status_Code,    
    claimpayment.Total_Claim_Charge_Amount,                 
    claimpayment.Claim_Payment_Amount,                      
    claimpayment.Patient_Responsibility_Amount,             
    claimpayment.Claim_Filing_Indicator_Code,              
    claimpayment.Payer_Claim_Control_Number,            
    claimpayment.Facility_Type_Code,                        
    claimpayment.Claim_Frequency_Code,                       
    clmpvt_pos_cd,
    clmpvt_pos_cd_instnl_typ_of_bll_cd,
    claimpayment.DRG_Code ,
    claimpayment.DRG_Weight,
    claimpayment.Discharge_Fraction,
    claimpayment.Rendering_Provider_NPI,
    claimpayment.Rendering_Provider_Identifier,
    claimpayment.State_License_Number,
    claimpayment.Provider_UPIN,
    claimpayment.Provider_Commerical_Number,
    claimpayment.Rendering_Provider_Last_or_Organization_Name,
    claimpayment.Rendering_Provider_First_Name,
    claimpayment.Coordination_of_Benefits_Carrier_Name,
    claimpayment.Coordination_of_Benefits_Carrier_Identifier,
    claimpayment.Identification_Code_Qualifier_2,
    claimpayment.Covered_Days_or_Visits_Count,
    claimpayment.PPS_Operaqting_Outlier_Amt,
    claimpayment.Lifetime_Psychiatric_Days_Count,
    claimpayment.Claim_DRG_Amt,
    claimpayment.Claim_Disproportionate_Share_Amt,
    claimpayment.Claim_MSP_Passthrough_Amt,
    claimpayment.Claim_PPS_Capital_Amt ,
    claimpayment.PPS_Capital_FSP_DRG_Amt_,
    claimpayment.PPS_Capital_HSP_DRG_Amt,
    claimpayment.PPS_Capital_DSH_DRG_Amt,
    claimpayment.Old_Capital_Amt  ,
    claimpayment.PPS_Capital_IME_Amt ,
    claimpayment.PPS_Operating_Hospital_Specific_DRG_Amt,
    claimpayment.Cost_Report_Day_Count,
    claimpayment.PPS_Operating_Federal_Specific_DRG_Amt,
    claimpayment.Claim_PPS_Capital_Outlier_Amt,
    claimpayment.Claim_Indirect_Teaching_Amt,
    claimpayment.Nonpayable_Professional_Component_Amt,
    claimpayment.PPS_Capital_Exception_Amt,
    claimpayment.Reimbursement_Rate,
    claimpayment.Claim_HCPCS_Payable_Amount,
    claimpayment.Claim_ESRD_Payment_Amount,
    clmpvt_clm_pymt_remrk_cd_seq_num,
    clmpvt_clm_pymt_remrk_cd, 
    clmpvt_clm_amt_seq_num,
    clmpvt_clm_amt,
    clmpvt_clm_amt_qual,
    clmpvt_clm_qty_seq_num,    
    clmpvt_clm_qty,
    clmpvt_clm_qty_qual,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_1, claimpayment.clmpvt_clm_adjmt_grp_cd_1, claimpayment.clmpvt_clm_adjmt_rsn_cd_1, claimpayment.clmpvt_clm_adjmt_amt_1, claimpayment.clmpvt_clm_adjmt_qty_1 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_2, claimpayment.clmpvt_clm_adjmt_grp_cd_2, claimpayment.clmpvt_clm_adjmt_rsn_cd_2, claimpayment.clmpvt_clm_adjmt_amt_2, claimpayment.clmpvt_clm_adjmt_qty_2 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_3, claimpayment.clmpvt_clm_adjmt_grp_cd_3, claimpayment.clmpvt_clm_adjmt_rsn_cd_3, claimpayment.clmpvt_clm_adjmt_amt_3, claimpayment.clmpvt_clm_adjmt_qty_3 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_4, claimpayment.clmpvt_clm_adjmt_grp_cd_4, claimpayment.clmpvt_clm_adjmt_rsn_cd_4, claimpayment.clmpvt_clm_adjmt_amt_4, claimpayment.clmpvt_clm_adjmt_qty_4 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_5, claimpayment.clmpvt_clm_adjmt_grp_cd_5, claimpayment.clmpvt_clm_adjmt_rsn_cd_5, claimpayment.clmpvt_clm_adjmt_amt_5, claimpayment.clmpvt_clm_adjmt_qty_5 )              
    ))[x.AdjGrp][0]                                      AS  clmpvt_clm_adjmt_seq_num ,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_1, claimpayment.clmpvt_clm_adjmt_grp_cd_1, claimpayment.clmpvt_clm_adjmt_rsn_cd_1, claimpayment.clmpvt_clm_adjmt_amt_1, claimpayment.clmpvt_clm_adjmt_qty_1 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_2, claimpayment.clmpvt_clm_adjmt_grp_cd_2, claimpayment.clmpvt_clm_adjmt_rsn_cd_2, claimpayment.clmpvt_clm_adjmt_amt_2, claimpayment.clmpvt_clm_adjmt_qty_2 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_3, claimpayment.clmpvt_clm_adjmt_grp_cd_3, claimpayment.clmpvt_clm_adjmt_rsn_cd_3, claimpayment.clmpvt_clm_adjmt_amt_3, claimpayment.clmpvt_clm_adjmt_qty_3 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_4, claimpayment.clmpvt_clm_adjmt_grp_cd_4, claimpayment.clmpvt_clm_adjmt_rsn_cd_4, claimpayment.clmpvt_clm_adjmt_amt_4, claimpayment.clmpvt_clm_adjmt_qty_4 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_5, claimpayment.clmpvt_clm_adjmt_grp_cd_5, claimpayment.clmpvt_clm_adjmt_rsn_cd_5, claimpayment.clmpvt_clm_adjmt_amt_5, claimpayment.clmpvt_clm_adjmt_qty_5 )             
    ))[x.AdjGrp][1]                                      AS  clmpvt_clm_adjmt_grp_cd ,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_1, claimpayment.clmpvt_clm_adjmt_grp_cd_1, claimpayment.clmpvt_clm_adjmt_rsn_cd_1, claimpayment.clmpvt_clm_adjmt_amt_1, claimpayment.clmpvt_clm_adjmt_qty_1 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_2, claimpayment.clmpvt_clm_adjmt_grp_cd_2, claimpayment.clmpvt_clm_adjmt_rsn_cd_2, claimpayment.clmpvt_clm_adjmt_amt_2, claimpayment.clmpvt_clm_adjmt_qty_2 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_3, claimpayment.clmpvt_clm_adjmt_grp_cd_3, claimpayment.clmpvt_clm_adjmt_rsn_cd_3, claimpayment.clmpvt_clm_adjmt_amt_3, claimpayment.clmpvt_clm_adjmt_qty_3 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_4, claimpayment.clmpvt_clm_adjmt_grp_cd_4, claimpayment.clmpvt_clm_adjmt_rsn_cd_4, claimpayment.clmpvt_clm_adjmt_amt_4, claimpayment.clmpvt_clm_adjmt_qty_4 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_5, claimpayment.clmpvt_clm_adjmt_grp_cd_5, claimpayment.clmpvt_clm_adjmt_rsn_cd_5, claimpayment.clmpvt_clm_adjmt_amt_5, claimpayment.clmpvt_clm_adjmt_qty_5 )                 
    ))[x.AdjGrp][2]                                      AS  clmpvt_clm_adjmt_rsn_txt ,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_1, claimpayment.clmpvt_clm_adjmt_grp_cd_1, claimpayment.clmpvt_clm_adjmt_rsn_cd_1, claimpayment.clmpvt_clm_adjmt_amt_1, claimpayment.clmpvt_clm_adjmt_qty_1 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_2, claimpayment.clmpvt_clm_adjmt_grp_cd_2, claimpayment.clmpvt_clm_adjmt_rsn_cd_2, claimpayment.clmpvt_clm_adjmt_amt_2, claimpayment.clmpvt_clm_adjmt_qty_2 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_3, claimpayment.clmpvt_clm_adjmt_grp_cd_3, claimpayment.clmpvt_clm_adjmt_rsn_cd_3, claimpayment.clmpvt_clm_adjmt_amt_3, claimpayment.clmpvt_clm_adjmt_qty_3 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_4, claimpayment.clmpvt_clm_adjmt_grp_cd_4, claimpayment.clmpvt_clm_adjmt_rsn_cd_4, claimpayment.clmpvt_clm_adjmt_amt_4, claimpayment.clmpvt_clm_adjmt_qty_4 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_5, claimpayment.clmpvt_clm_adjmt_grp_cd_5, claimpayment.clmpvt_clm_adjmt_rsn_cd_5, claimpayment.clmpvt_clm_adjmt_amt_5, claimpayment.clmpvt_clm_adjmt_qty_5 )                
    ))[x.AdjGrp][3]                                      AS  clmpvt_clm_adjmt_amt ,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_1, claimpayment.clmpvt_clm_adjmt_grp_cd_1, claimpayment.clmpvt_clm_adjmt_rsn_cd_1, claimpayment.clmpvt_clm_adjmt_amt_1, claimpayment.clmpvt_clm_adjmt_qty_1 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_2, claimpayment.clmpvt_clm_adjmt_grp_cd_2, claimpayment.clmpvt_clm_adjmt_rsn_cd_2, claimpayment.clmpvt_clm_adjmt_amt_2, claimpayment.clmpvt_clm_adjmt_qty_2 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_3, claimpayment.clmpvt_clm_adjmt_grp_cd_3, claimpayment.clmpvt_clm_adjmt_rsn_cd_3, claimpayment.clmpvt_clm_adjmt_amt_3, claimpayment.clmpvt_clm_adjmt_qty_3 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_4, claimpayment.clmpvt_clm_adjmt_grp_cd_4, claimpayment.clmpvt_clm_adjmt_rsn_cd_4, claimpayment.clmpvt_clm_adjmt_amt_4, claimpayment.clmpvt_clm_adjmt_qty_4 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_5, claimpayment.clmpvt_clm_adjmt_grp_cd_5, claimpayment.clmpvt_clm_adjmt_rsn_cd_5, claimpayment.clmpvt_clm_adjmt_amt_5, claimpayment.clmpvt_clm_adjmt_qty_5 )               
    ))[x.AdjGrp][4]                                      AS  clmpvt_clm_adjmt_qty
FROM veradigm_era_claimpayment_pivot_pre claimpayment
 CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS AdjGrp) x 
WHERE 
--header.batch_id  IN ('201707171118261600000001000879' , '201707201911567400000050000879','201707011222349000000051000979')
        (densify_2d_array(ARRAY(
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_1, claimpayment.clmpvt_clm_adjmt_grp_cd_1, claimpayment.clmpvt_clm_adjmt_rsn_cd_1, claimpayment.clmpvt_clm_adjmt_amt_1, claimpayment.clmpvt_clm_adjmt_qty_1 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_2, claimpayment.clmpvt_clm_adjmt_grp_cd_2, claimpayment.clmpvt_clm_adjmt_rsn_cd_2, claimpayment.clmpvt_clm_adjmt_amt_2, claimpayment.clmpvt_clm_adjmt_qty_2 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_3, claimpayment.clmpvt_clm_adjmt_grp_cd_3, claimpayment.clmpvt_clm_adjmt_rsn_cd_3, claimpayment.clmpvt_clm_adjmt_amt_3, claimpayment.clmpvt_clm_adjmt_qty_3 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_4, claimpayment.clmpvt_clm_adjmt_grp_cd_4, claimpayment.clmpvt_clm_adjmt_rsn_cd_4, claimpayment.clmpvt_clm_adjmt_amt_4, claimpayment.clmpvt_clm_adjmt_qty_4 ),
            ARRAY(claimpayment.clmpvt_clm_adjmt_seq_num_5, claimpayment.clmpvt_clm_adjmt_grp_cd_5, claimpayment.clmpvt_clm_adjmt_rsn_cd_5, claimpayment.clmpvt_clm_adjmt_amt_5, claimpayment.clmpvt_clm_adjmt_qty_5 )            
        ))[x.AdjGrp] != CAST(ARRAY(NULL, NULL, NULL, NULL, NULL) AS ARRAY<STRING>) OR x.AdjGrp = 0)
-- AND 
-- batch_id  IN ('201707281308100400000004000879')
