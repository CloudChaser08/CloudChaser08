SELECT
    header.Batch_ID,
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
    OBSCURE_PLACE_OF_SERVICE(
       CASE WHEN LENGTH(TRIM(CONCAT(COALESCE(claimpayment.Facility_Type_Code,''), 
                                    COALESCE(claimpayment.Claim_Frequency_Code,'')))) IN (1,2) 
            THEN  RIGHT(CONCAT('00', 
              CONCAT(COALESCE(claimpayment.Facility_Type_Code,''), 
                     COALESCE(claimpayment.Claim_Frequency_Code,''))),2)
       ELSE NULL END)                                     AS clmpvt_pos_cd,
  OBSCURE_INST_TYPE_OF_BILL(
       CASE WHEN LENGTH(TRIM(COALESCE(CONCAT(claimpayment.Facility_Type_Code, Claim_Frequency_Code),''))) = 3 
            THEN  CONCAT(claimpayment.Facility_Type_Code, claimpayment.Claim_Frequency_Code) 
       ELSE NULL END)                                     AS clmpvt_pos_cd_instnl_typ_of_bll_cd,
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
  CASE WHEN LENGTH(TRIM(COALESCE
           (
            claimpayment.Remark_Code_1,
            claimpayment.Remark_Code_2,
            claimpayment.Remark_Code_3,
            claimpayment.Remark_Code_4,
            claimpayment.Remark_Code_5,
            claimpayment.Remark_Code_6,
            claimpayment.Remark_Code_7,
            claimpayment.Remark_Code_8,
            claimpayment.Remark_Code_9,
            claimpayment.Remark_Code_10,''
           ))) <> 0  THEN RemarkCodeEx.n + 1 ELSE NULL END AS clmpvt_clm_pymt_remrk_cd_seq_num,
    UPPER(ARRAY(
            claimpayment.Remark_Code_1,
            claimpayment.Remark_Code_2,
            claimpayment.Remark_Code_3,
            claimpayment.Remark_Code_4,
            claimpayment.Remark_Code_5,
            claimpayment.Remark_Code_6,
            claimpayment.Remark_Code_7,
            claimpayment.Remark_Code_8,
            claimpayment.Remark_Code_9,
            claimpayment.Remark_Code_10
               )[RemarkCodeEx.n]
           )                                              AS clmpvt_clm_pymt_remrk_cd, 

  CASE WHEN
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Total_Covered_Charges,                                   claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_1),
            ARRAY(claimpayment.Prompt_Pay_Discount_Amount,                              claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_2),
            ARRAY(claimpayment.Per_Day_Limit,                                           claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_3),
            ARRAY(claimpayment.Patient_Amount_Paid,                                     claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_4),
            ARRAY(claimpayment.Interest,                                                claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_5),
            ARRAY(claimpayment.Negative_Ledger_Balance_Medicare_Pt_A_Pt_B_only,         claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_6),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_1, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_9),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_2, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_10),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_3, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_11),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_4, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_12),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_5, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_13),
            ARRAY(claimpayment.Mutually_Defined_Medicare_Pt_A_Operational_Cost_or_Day_Outlier_Amt,claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_15)
    ))[x.AmountEx][0] IS NULL THEN NULL ELSE x.AmountEx + 1 END  AS clmpvt_clm_amt_seq_num,
  densify_2d_array(ARRAY(
            ARRAY(claimpayment.Total_Covered_Charges,                                   claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_1),
            ARRAY(claimpayment.Prompt_Pay_Discount_Amount,                              claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_2),
            ARRAY(claimpayment.Per_Day_Limit,                                           claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_3),
            ARRAY(claimpayment.Patient_Amount_Paid,                                     claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_4),
            ARRAY(claimpayment.Interest,                                                claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_5),
            ARRAY(claimpayment.Negative_Ledger_Balance_Medicare_Pt_A_Pt_B_only,         claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_6),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_1, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_9),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_2, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_10),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_3, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_11),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_4, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_12),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_5, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_13),
            ARRAY(claimpayment.Mutually_Defined_Medicare_Pt_A_Operational_Cost_or_Day_Outlier_Amt,claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_15)
    ))[x.AmountEx][0]                                      AS clmpvt_clm_amt,
  densify_2d_array(ARRAY(
            ARRAY(claimpayment.Total_Covered_Charges,                                   claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_1),
            ARRAY(claimpayment.Prompt_Pay_Discount_Amount,                              claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_2),
            ARRAY(claimpayment.Per_Day_Limit,                                           claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_3),
            ARRAY(claimpayment.Patient_Amount_Paid,                                     claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_4),
            ARRAY(claimpayment.Interest,                                                claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_5),
            ARRAY(claimpayment.Negative_Ledger_Balance_Medicare_Pt_A_Pt_B_only,         claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_6),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_1, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_9),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_2, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_10),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_3, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_11),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_4, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_12),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_5, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_13),
            ARRAY(claimpayment.Mutually_Defined_Medicare_Pt_A_Operational_Cost_or_Day_Outlier_Amt,claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_15)
    ))[x.AmountEx][1]                                      AS clmpvt_clm_amt_qual,
  CASE WHEN
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Covered_Actual,                                          claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_16),
            ARRAY(claimpayment.CoInsured_Actual,                                        claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_17),
            ARRAY(claimpayment.Actual_Lifetime_Reserve,                                 claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_18),
            ARRAY(claimpayment.Estimated_Lifetime_Reserve_,                             claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_19),
            ARRAY(claimpayment.Noncovered_Days,                                         claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_20),
            ARRAY(claimpayment.Estimated_Noncovered,                                    claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_21),
            ARRAY(claimpayment.Not_Replaced_Blood_Units,                                claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_22),
            ARRAY(claimpayment.Outlier_Days,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_23),
            ARRAY(claimpayment.Prescription,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_24),
            ARRAY(claimpayment.Visits,                                                  claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_25),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_6, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_26),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_7, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_27),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_8, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_28),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_9, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_29),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_10,claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_30)
    ))[x2.QtyEx][0] IS NULL THEN NULL ELSE   x2.QtyEx + 1   END  AS clmpvt_clm_qty_seq_num,    
  densify_2d_array(ARRAY(
            ARRAY(claimpayment.Covered_Actual,                                          claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_16),
            ARRAY(claimpayment.CoInsured_Actual,                                        claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_17),
            ARRAY(claimpayment.Actual_Lifetime_Reserve,                                 claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_18),
            ARRAY(claimpayment.Estimated_Lifetime_Reserve_,                             claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_19),
            ARRAY(claimpayment.Noncovered_Days,                                         claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_20),
            ARRAY(claimpayment.Estimated_Noncovered,                                    claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_21),
            ARRAY(claimpayment.Not_Replaced_Blood_Units,                                claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_22),
            ARRAY(claimpayment.Outlier_Days,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_23),
            ARRAY(claimpayment.Prescription,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_24),
            ARRAY(claimpayment.Visits,                                                  claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_25),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_6, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_26),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_7, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_27),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_8, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_28),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_9, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_29),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_10,claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_30)
    ))[x2.QtyEx][0]                                       AS clmpvt_clm_qty,
   densify_2d_array(ARRAY(
            ARRAY(claimpayment.Covered_Actual,                                          claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_16),
            ARRAY(claimpayment.CoInsured_Actual,                                        claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_17),
            ARRAY(claimpayment.Actual_Lifetime_Reserve,                                 claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_18),
            ARRAY(claimpayment.Estimated_Lifetime_Reserve_,                             claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_19),
            ARRAY(claimpayment.Noncovered_Days,                                         claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_20),
            ARRAY(claimpayment.Estimated_Noncovered,                                    claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_21),
            ARRAY(claimpayment.Not_Replaced_Blood_Units,                                claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_22),
            ARRAY(claimpayment.Outlier_Days,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_23),
            ARRAY(claimpayment.Prescription,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_24),
            ARRAY(claimpayment.Visits,                                                  claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_25),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_6, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_26),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_7, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_27),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_8, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_28),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_9, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_29),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_10,claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_30)
    ))[x2.QtyEx][1]                                      AS clmpvt_clm_qty_qual,

    CASE WHEN claimpayment.Claim_Adjustment_Group_Code_1 IS NOT NULL
    THEN 1
    ELSE NULL END                                       AS clmpvt_clm_adjmt_seq_num_1, 
    claimpayment.Claim_Adjustment_Group_Code_1          AS clmpvt_clm_adjmt_grp_cd_1,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_1_Reason_1, claimpayment.Adj_Group_1_Amt_1, claimpayment.Adj_Group_1_Qty_1),
            ARRAY(claimpayment.Adj_Group_1_Reason_2, claimpayment.Adj_Group_1_Amt_2, claimpayment.Adj_Group_1_Qty_2),
            ARRAY(claimpayment.Adj_Group_1_Reason_3, claimpayment.Adj_Group_1_Amt_3, claimpayment.Adj_Group_1_Qty_3),
            ARRAY(claimpayment.Adj_Group_1_Reason_4, claimpayment.Adj_Group_1_Amt_4, claimpayment.Adj_Group_1_Qty_4),
            ARRAY(claimpayment.Adj_Group_1_Reason_5, claimpayment.Adj_Group_1_Amt_5, claimpayment.Adj_Group_1_Qty_5),
            ARRAY(claimpayment.Adj_Group_1_Reason_6, claimpayment.Adj_Group_1_Amt_6, claimpayment.Adj_Group_1_Qty_6)
    ))[x.AdjGrp_1][0]                                      AS clmpvt_clm_adjmt_rsn_cd_1,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_1_Reason_1, claimpayment.Adj_Group_1_Amt_1, claimpayment.Adj_Group_1_Qty_1),
            ARRAY(claimpayment.Adj_Group_1_Reason_2, claimpayment.Adj_Group_1_Amt_2, claimpayment.Adj_Group_1_Qty_2),
            ARRAY(claimpayment.Adj_Group_1_Reason_3, claimpayment.Adj_Group_1_Amt_3, claimpayment.Adj_Group_1_Qty_3),
            ARRAY(claimpayment.Adj_Group_1_Reason_4, claimpayment.Adj_Group_1_Amt_4, claimpayment.Adj_Group_1_Qty_4),
            ARRAY(claimpayment.Adj_Group_1_Reason_5, claimpayment.Adj_Group_1_Amt_5, claimpayment.Adj_Group_1_Qty_5),
            ARRAY(claimpayment.Adj_Group_1_Reason_6, claimpayment.Adj_Group_1_Amt_6, claimpayment.Adj_Group_1_Qty_6)
    ))[x.AdjGrp_1][1]                                      AS clmpvt_clm_adjmt_amt_1,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_1_Reason_1, claimpayment.Adj_Group_1_Amt_1, claimpayment.Adj_Group_1_Qty_1),
            ARRAY(claimpayment.Adj_Group_1_Reason_2, claimpayment.Adj_Group_1_Amt_2, claimpayment.Adj_Group_1_Qty_2),
            ARRAY(claimpayment.Adj_Group_1_Reason_3, claimpayment.Adj_Group_1_Amt_3, claimpayment.Adj_Group_1_Qty_3),
            ARRAY(claimpayment.Adj_Group_1_Reason_4, claimpayment.Adj_Group_1_Amt_4, claimpayment.Adj_Group_1_Qty_4),
            ARRAY(claimpayment.Adj_Group_1_Reason_5, claimpayment.Adj_Group_1_Amt_5, claimpayment.Adj_Group_1_Qty_5),
            ARRAY(claimpayment.Adj_Group_1_Reason_6, claimpayment.Adj_Group_1_Amt_6, claimpayment.Adj_Group_1_Qty_6)
    ))[x.AdjGrp_1][2]                                      AS clmpvt_clm_adjmt_qty_1,
    CASE WHEN claimpayment.Claim_Adjustment_Group_Code_2 IS NOT NULL
    THEN 2
    ELSE NULL END                                          AS  clmpvt_clm_adjmt_seq_num_2, 
    claimpayment.Claim_Adjustment_Group_Code_2             AS clmpvt_clm_adjmt_grp_cd_2,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_2_Reason_1, claimpayment.Adj_Group_2_Amt_1, claimpayment.Adj_Group_2_Qty_1),
            ARRAY(claimpayment.Adj_Group_2_Reason_2, claimpayment.Adj_Group_2_Amt_2, claimpayment.Adj_Group_2_Qty_2),
            ARRAY(claimpayment.Adj_Group_2_Reason_3, claimpayment.Adj_Group_2_Amt_3, claimpayment.Adj_Group_2_Qty_3),
            ARRAY(claimpayment.Adj_Group_2_Reason_4, claimpayment.Adj_Group_2_Amt_4, claimpayment.Adj_Group_2_Qty_4),
            ARRAY(claimpayment.Adj_Group_2_Reason_5, claimpayment.Adj_Group_2_Amt_5, claimpayment.Adj_Group_2_Qty_5),
            ARRAY(claimpayment.Adj_Group_2_Reason_6, claimpayment.Adj_Group_2_Amt_6, claimpayment.Adj_Group_2_Qty_6)
    ))[x.AdjGrp_2][0]                                      AS clmpvt_clm_adjmt_rsn_cd_2,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_2_Reason_1, claimpayment.Adj_Group_2_Amt_1, claimpayment.Adj_Group_2_Qty_1),
            ARRAY(claimpayment.Adj_Group_2_Reason_2, claimpayment.Adj_Group_2_Amt_2, claimpayment.Adj_Group_2_Qty_2),
            ARRAY(claimpayment.Adj_Group_2_Reason_3, claimpayment.Adj_Group_2_Amt_3, claimpayment.Adj_Group_2_Qty_3),
            ARRAY(claimpayment.Adj_Group_2_Reason_4, claimpayment.Adj_Group_2_Amt_4, claimpayment.Adj_Group_2_Qty_4),
            ARRAY(claimpayment.Adj_Group_2_Reason_5, claimpayment.Adj_Group_2_Amt_5, claimpayment.Adj_Group_2_Qty_5),
            ARRAY(claimpayment.Adj_Group_2_Reason_6, claimpayment.Adj_Group_2_Amt_6, claimpayment.Adj_Group_2_Qty_6)
    ))[x.AdjGrp_2][1]                                      AS clmpvt_clm_adjmt_amt_2,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_2_Reason_1, claimpayment.Adj_Group_2_Amt_1, claimpayment.Adj_Group_2_Qty_1),
            ARRAY(claimpayment.Adj_Group_2_Reason_2, claimpayment.Adj_Group_2_Amt_2, claimpayment.Adj_Group_2_Qty_2),
            ARRAY(claimpayment.Adj_Group_2_Reason_3, claimpayment.Adj_Group_2_Amt_3, claimpayment.Adj_Group_2_Qty_3),
            ARRAY(claimpayment.Adj_Group_2_Reason_4, claimpayment.Adj_Group_2_Amt_4, claimpayment.Adj_Group_2_Qty_4),
            ARRAY(claimpayment.Adj_Group_2_Reason_5, claimpayment.Adj_Group_2_Amt_5, claimpayment.Adj_Group_2_Qty_5),
            ARRAY(claimpayment.Adj_Group_2_Reason_6, claimpayment.Adj_Group_2_Amt_6, claimpayment.Adj_Group_2_Qty_6)
    ))[x.AdjGrp_2][2]                                      AS clmpvt_clm_adjmt_qty_2,
    CASE WHEN claimpayment.Claim_Adjustment_Group_Code_3 IS NOT NULL
    THEN 3
    ELSE NULL END                                          AS clmpvt_clm_adjmt_seq_num_3, 
    claimpayment.Claim_Adjustment_Group_Code_3             AS clmpvt_clm_adjmt_grp_cd_3,    
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_3_Reason_1, claimpayment.Adj_Group_3_Amt_1, claimpayment.Adj_Group_3_Qty_1),
            ARRAY(claimpayment.Adj_Group_3_Reason_2, claimpayment.Adj_Group_3_Amt_2, claimpayment.Adj_Group_3_Qty_2),
            ARRAY(claimpayment.Adj_Group_3_Reason_3, claimpayment.Adj_Group_3_Amt_3, claimpayment.Adj_Group_3_Qty_3),
            ARRAY(claimpayment.Adj_Group_3_Reason_4, claimpayment.Adj_Group_3_Amt_4, claimpayment.Adj_Group_3_Qty_4),
            ARRAY(claimpayment.Adj_Group_3_Reason_5, claimpayment.Adj_Group_3_Amt_5, claimpayment.Adj_Group_3_Qty_5),
            ARRAY(claimpayment.Adj_Group_3_Reason_6, claimpayment.Adj_Group_3_Amt_6, claimpayment.Adj_Group_3_Qty_6)
    ))[x.AdjGrp_3][0]                                      AS clmpvt_clm_adjmt_rsn_cd_3,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_3_Reason_1, claimpayment.Adj_Group_3_Amt_1, claimpayment.Adj_Group_3_Qty_1),
            ARRAY(claimpayment.Adj_Group_3_Reason_2, claimpayment.Adj_Group_3_Amt_2, claimpayment.Adj_Group_3_Qty_2),
            ARRAY(claimpayment.Adj_Group_3_Reason_3, claimpayment.Adj_Group_3_Amt_3, claimpayment.Adj_Group_3_Qty_3),
            ARRAY(claimpayment.Adj_Group_3_Reason_4, claimpayment.Adj_Group_3_Amt_4, claimpayment.Adj_Group_3_Qty_4),
            ARRAY(claimpayment.Adj_Group_3_Reason_5, claimpayment.Adj_Group_3_Amt_5, claimpayment.Adj_Group_3_Qty_5),
            ARRAY(claimpayment.Adj_Group_3_Reason_6, claimpayment.Adj_Group_3_Amt_6, claimpayment.Adj_Group_3_Qty_6)
    ))[x.AdjGrp_3][1]                                      AS clmpvt_clm_adjmt_amt_3,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_3_Reason_1, claimpayment.Adj_Group_3_Amt_1, claimpayment.Adj_Group_3_Qty_1),
            ARRAY(claimpayment.Adj_Group_3_Reason_2, claimpayment.Adj_Group_3_Amt_2, claimpayment.Adj_Group_3_Qty_2),
            ARRAY(claimpayment.Adj_Group_3_Reason_3, claimpayment.Adj_Group_3_Amt_3, claimpayment.Adj_Group_3_Qty_3),
            ARRAY(claimpayment.Adj_Group_3_Reason_4, claimpayment.Adj_Group_3_Amt_4, claimpayment.Adj_Group_3_Qty_4),
            ARRAY(claimpayment.Adj_Group_3_Reason_5, claimpayment.Adj_Group_3_Amt_5, claimpayment.Adj_Group_3_Qty_5),
            ARRAY(claimpayment.Adj_Group_3_Reason_6, claimpayment.Adj_Group_3_Amt_6, claimpayment.Adj_Group_3_Qty_6)
    ))[x.AdjGrp_3][2]                                      AS clmpvt_clm_adjmt_qty_3,
    CASE WHEN claimpayment.Claim_Adjustment_Group_Code_3 IS NOT NULL
    THEN 4
    ELSE NULL END                                          AS clmpvt_clm_adjmt_seq_num_4, 
    claimpayment.Claim_Adjustment_Group_Code_4             AS clmpvt_clm_adjmt_grp_cd_4,      
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_4_Reason_1, claimpayment.Adj_Group_4_Amt_1, claimpayment.Adj_Group_4_Qty_1),
            ARRAY(claimpayment.Adj_Group_4_Reason_2, claimpayment.Adj_Group_4_Amt_2, claimpayment.Adj_Group_4_Qty_2),
            ARRAY(claimpayment.Adj_Group_4_Reason_3, claimpayment.Adj_Group_4_Amt_3, claimpayment.Adj_Group_4_Qty_3),
            ARRAY(claimpayment.Adj_Group_4_Reason_4, claimpayment.Adj_Group_4_Amt_4, claimpayment.Adj_Group_4_Qty_4),
            ARRAY(claimpayment.Adj_Group_4_Reason_5, claimpayment.Adj_Group_4_Amt_5, claimpayment.Adj_Group_4_Qty_5),
            ARRAY(claimpayment.Adj_Group_4_Reason_6, claimpayment.Adj_Group_4_Amt_6, claimpayment.Adj_Group_4_Qty_6)
    ))[x.AdjGrp_4][0]                                      AS clmpvt_clm_adjmt_rsn_cd_4,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_4_Reason_1, claimpayment.Adj_Group_4_Amt_1, claimpayment.Adj_Group_4_Qty_1),
            ARRAY(claimpayment.Adj_Group_4_Reason_2, claimpayment.Adj_Group_4_Amt_2, claimpayment.Adj_Group_4_Qty_2),
            ARRAY(claimpayment.Adj_Group_4_Reason_3, claimpayment.Adj_Group_4_Amt_3, claimpayment.Adj_Group_4_Qty_3),
            ARRAY(claimpayment.Adj_Group_4_Reason_4, claimpayment.Adj_Group_4_Amt_4, claimpayment.Adj_Group_4_Qty_4),
            ARRAY(claimpayment.Adj_Group_4_Reason_5, claimpayment.Adj_Group_4_Amt_5, claimpayment.Adj_Group_4_Qty_5),
            ARRAY(claimpayment.Adj_Group_4_Reason_6, claimpayment.Adj_Group_4_Amt_6, claimpayment.Adj_Group_4_Qty_6)
    ))[x.AdjGrp_4][1]                                      AS clmpvt_clm_adjmt_amt_4,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_4_Reason_1, claimpayment.Adj_Group_4_Amt_1, claimpayment.Adj_Group_4_Qty_1),
            ARRAY(claimpayment.Adj_Group_4_Reason_2, claimpayment.Adj_Group_4_Amt_2, claimpayment.Adj_Group_4_Qty_2),
            ARRAY(claimpayment.Adj_Group_4_Reason_3, claimpayment.Adj_Group_4_Amt_3, claimpayment.Adj_Group_4_Qty_3),
            ARRAY(claimpayment.Adj_Group_4_Reason_4, claimpayment.Adj_Group_4_Amt_4, claimpayment.Adj_Group_4_Qty_4),
            ARRAY(claimpayment.Adj_Group_4_Reason_5, claimpayment.Adj_Group_4_Amt_5, claimpayment.Adj_Group_4_Qty_5),
            ARRAY(claimpayment.Adj_Group_4_Reason_6, claimpayment.Adj_Group_4_Amt_6, claimpayment.Adj_Group_4_Qty_6)
    ))[x.AdjGrp_4][2]                                      AS clmpvt_clm_adjmt_qty_4,
    CASE WHEN claimpayment.Claim_Adjustment_Group_Code_5 IS NOT NULL
    THEN 5
    ELSE NULL END                                          AS clmpvt_clm_adjmt_seq_num_5, 
    claimpayment.Claim_Adjustment_Group_Code_5             AS clmpvt_clm_adjmt_grp_cd_5,  
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_5_Reason_1, claimpayment.Adj_Group_5_Amt_1, claimpayment.Adj_Group_5_Qty_1),
            ARRAY(claimpayment.Adj_Group_5_Reason_2, claimpayment.Adj_Group_5_Amt_2, claimpayment.Adj_Group_5_Qty_2),
            ARRAY(claimpayment.Adj_Group_5_Reason_3, claimpayment.Adj_Group_5_Amt_3, claimpayment.Adj_Group_5_Qty_3),
            ARRAY(claimpayment.Adj_Group_5_Reason_4, claimpayment.Adj_Group_5Amt_4, claimpayment.Adj_Group_5_Qty_4),
            ARRAY(claimpayment.Adj_Group_5_Reason_5, claimpayment.Adj_Group_5_Amt_5, claimpayment.Adj_Group_5_Qty_5),
            ARRAY(claimpayment.Adj_Group_5_Reason_6, claimpayment.Adj_Group_5_Amt_6, claimpayment.Adj_Group_5_Qty_6)
    ))[x.AdjGrp_5][0]                                      AS clmpvt_clm_adjmt_rsn_cd_5,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_5_Reason_1, claimpayment.Adj_Group_5_Amt_1, claimpayment.Adj_Group_5_Qty_1),
            ARRAY(claimpayment.Adj_Group_5_Reason_2, claimpayment.Adj_Group_5_Amt_2, claimpayment.Adj_Group_5_Qty_2),
            ARRAY(claimpayment.Adj_Group_5_Reason_3, claimpayment.Adj_Group_5_Amt_3, claimpayment.Adj_Group_5_Qty_3),
            ARRAY(claimpayment.Adj_Group_5_Reason_4, claimpayment.Adj_Group_5Amt_4, claimpayment.Adj_Group_5_Qty_4),
            ARRAY(claimpayment.Adj_Group_5_Reason_5, claimpayment.Adj_Group_5_Amt_5, claimpayment.Adj_Group_5_Qty_5),
            ARRAY(claimpayment.Adj_Group_5_Reason_6, claimpayment.Adj_Group_5_Amt_6, claimpayment.Adj_Group_5_Qty_6)
    ))[x.AdjGrp_5][1]                                      AS clmpvt_clm_adjmt_amt_5,
    densify_2d_array(ARRAY(
            ARRAY(claimpayment.Adj_Group_5_Reason_1, claimpayment.Adj_Group_5_Amt_1, claimpayment.Adj_Group_5_Qty_1),
            ARRAY(claimpayment.Adj_Group_5_Reason_2, claimpayment.Adj_Group_5_Amt_2, claimpayment.Adj_Group_5_Qty_2),
            ARRAY(claimpayment.Adj_Group_5_Reason_3, claimpayment.Adj_Group_5_Amt_3, claimpayment.Adj_Group_5_Qty_3),
            ARRAY(claimpayment.Adj_Group_5_Reason_4, claimpayment.Adj_Group_5Amt_4, claimpayment.Adj_Group_5_Qty_4),
            ARRAY(claimpayment.Adj_Group_5_Reason_5, claimpayment.Adj_Group_5_Amt_5, claimpayment.Adj_Group_5_Qty_5),
            ARRAY(claimpayment.Adj_Group_5_Reason_6, claimpayment.Adj_Group_5_Amt_6, claimpayment.Adj_Group_5_Qty_6)
    ))[x.AdjGrp_5][2]                                      AS clmpvt_clm_adjmt_qty_5


FROM hdr header
  LEFT OUTER JOIN payload era_payload
    ON header.Batch_ID = era_payload.claimid
  LEFT OUTER JOIN clp claimpayment
    ON matching_payload.hvJoinKey = claimpayment.hvJoinKey
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)) AS n) RemarkCodeEx
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS AmountEx) x
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14)) AS QtyEx) x2
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS AdjGrp_1) x
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS AdjGrp_2) x
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS AdjGrp_3) x
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS AdjGrp_4) x
  CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS AdjGrp_5) x
WHERE header.Header_Indicator='HDR'
AND
        (
          LENGTH(TRIM(COALESCE(ARRAY
           (
            claimpayment.Remark_Code_1,
            claimpayment.Remark_Code_2,
            claimpayment.Remark_Code_3,
            claimpayment.Remark_Code_4,
            claimpayment.Remark_Code_5,
            claimpayment.Remark_Code_6,
            claimpayment.Remark_Code_7,
            claimpayment.Remark_Code_8,
            claimpayment.Remark_Code_9,
            claimpayment.Remark_Code_10,''
            )[RemarkCodeEx.n]))) <> 0 -- IS NOT NULL
         OR  
         LENGTH(TRIM(COALESCE
           (
            claimpayment.Remark_Code_1,
            claimpayment.Remark_Code_2,
            claimpayment.Remark_Code_3,
            claimpayment.Remark_Code_4,
            claimpayment.Remark_Code_5,
            claimpayment.Remark_Code_6,
            claimpayment.Remark_Code_7,
            claimpayment.Remark_Code_8,
            claimpayment.Remark_Code_9,
            claimpayment.Remark_Code_10,''
           ))) = 0  AND RemarkCodeEx.n = 0
       ) 
----------- Amount Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(claimpayment.Total_Covered_Charges,                                   claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_1),
            ARRAY(claimpayment.Prompt_Pay_Discount_Amount,                              claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_2),
            ARRAY(claimpayment.Per_Day_Limit,                                           claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_3),
            ARRAY(claimpayment.Patient_Amount_Paid,                                     claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_4),
            ARRAY(claimpayment.Interest,                                                claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_5),
            ARRAY(claimpayment.Negative_Ledger_Balance_Medicare_Pt_A_Pt_B_only,         claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_6),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_1, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_9),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_2, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_10),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_3, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_11),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_4, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_12),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_5, claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_13),
            ARRAY(claimpayment.Mutually_Defined_Medicare_Pt_A_Operational_Cost_or_Day_Outlier_Amt,claimpayment.Claim_Supplemental_Information_Amount_Identifier_Informational_15)
      ))[x.AmountEx] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x.AmountEx = 0
    )
----------- Qty Explosion
AND
    (densify_2d_array
      (ARRAY(
           ARRAY(claimpayment.Covered_Actual,                                           claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_16),
            ARRAY(claimpayment.CoInsured_Actual,                                        claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_17),
            ARRAY(claimpayment.Actual_Lifetime_Reserve,                                 claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_18),
            ARRAY(claimpayment.Estimated_Lifetime_Reserve_,                             claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_19),
            ARRAY(claimpayment.Noncovered_Days,                                         claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_20),
            ARRAY(claimpayment.Estimated_Noncovered,                                    claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_21),
            ARRAY(claimpayment.Not_Replaced_Blood_Units,                                claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_22),
            ARRAY(claimpayment.Outlier_Days,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_23),
            ARRAY(claimpayment.Prescription,                                            claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_24),
            ARRAY(claimpayment.Visits,                                                  claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_25),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_6, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_26),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_7, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_27),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_8, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_28),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_9, claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_29),
            ARRAY(claimpayment.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_10,claimpayment.Claim_Supplemental_Information_Quantity_Identifier_Informational_30)
      ))[x2.QtyEx] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x2.QtyEx = 0
    )
----------- Adjustment Group 1 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(claimpayment.Adj_Group_1_Reason_1, Adj_Group_1_Amt_1, Adj_Group_1_Qty_1),
            ARRAY(claimpayment.Adj_Group_1_Reason_2, Adj_Group_1_Amt_2, Adj_Group_1_Qty_2),
            ARRAY(claimpayment.Adj_Group_1_Reason_3, Adj_Group_1_Amt_3, Adj_Group_1_Qty_3),
            ARRAY(claimpayment.Adj_Group_1_Reason_4, Adj_Group_1_Amt_4, Adj_Group_1_Qty_4),
            ARRAY(claimpayment.Adj_Group_1_Reason_5, Adj_Group_1_Amt_5, Adj_Group_1_Qty_5),
            ARRAY(claimpayment.Adj_Group_1_Reason_6, Adj_Group_1_Amt_6, Adj_Group_1_Qty_6)
        ))[x.AdjGrp_1] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.AdjGrp_1 = 0
    )
----------- Adjustment Group 2 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(claimpayment.Adj_Group_2_Reason_1, Adj_Group_2_Amt_1, Adj_Group_2_Qty_1),
            ARRAY(claimpayment.Adj_Group_2_Reason_2, Adj_Group_2_Amt_2, Adj_Group_2_Qty_2),
            ARRAY(claimpayment.Adj_Group_2_Reason_3, Adj_Group_2_Amt_3, Adj_Group_2_Qty_3),
            ARRAY(claimpayment.Adj_Group_2_Reason_4, Adj_Group_2_Amt_4, Adj_Group_2_Qty_4),
            ARRAY(claimpayment.Adj_Group_2_Reason_5, Adj_Group_2_Amt_5, Adj_Group_2_Qty_5),
            ARRAY(claimpayment.Adj_Group_2_Reason_6, Adj_Group_2_Amt_6, Adj_Group_2_Qty_6)
        ))[x.AdjGrp_2] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.AdjGrp_2 = 0
    )
----------- Adjustment Group 3 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(claimpayment.Adj_Group_3_Reason_1, Adj_Group_3_Amt_1, Adj_Group_3_Qty_1),
            ARRAY(claimpayment.Adj_Group_3_Reason_2, Adj_Group_3_Amt_2, Adj_Group_3_Qty_2),
            ARRAY(claimpayment.Adj_Group_3_Reason_3, Adj_Group_3_Amt_3, Adj_Group_3_Qty_3),
            ARRAY(claimpayment.Adj_Group_3_Reason_4, Adj_Group_3_Amt_4, Adj_Group_3_Qty_4),
            ARRAY(claimpayment.Adj_Group_3_Reason_5, Adj_Group_3_Amt_5, Adj_Group_3_Qty_5),
            ARRAY(claimpayment.Adj_Group_3_Reason_6, Adj_Group_3_Amt_6, Adj_Group_3_Qty_6)
        ))[x.AdjGrp_3] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.AdjGrp_3 = 0
    )
----------- Adjustment Group 4 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(claimpayment.Adj_Group_4_Reason_1, Adj_Group_4_Amt_1, Adj_Group_4_Qty_1),
            ARRAY(claimpayment.Adj_Group_4_Reason_2, Adj_Group_4_Amt_2, Adj_Group_4_Qty_2),
            ARRAY(claimpayment.Adj_Group_4_Reason_3, Adj_Group_4_Amt_3, Adj_Group_4_Qty_3),
            ARRAY(claimpayment.Adj_Group_4_Reason_4, Adj_Group_4_Amt_4, Adj_Group_4_Qty_4),
            ARRAY(claimpayment.Adj_Group_4_Reason_5, Adj_Group_4_Amt_5, Adj_Group_4_Qty_5),
            ARRAY(claimpayment.Adj_Group_4_Reason_6, Adj_Group_4_Amt_6, Adj_Group_4_Qty_6)
        ))[x.AdjGrp_4] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.AdjGrp_4 = 0
    )
----------- Adjustment Group 5 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(claimpayment.Adj_Group_5_Reason_1, Adj_Group_5_Amt_1, Adj_Group_5_Qty_1),
            ARRAY(claimpayment.Adj_Group_5_Reason_2, Adj_Group_5_Amt_2, Adj_Group_5_Qty_2),
            ARRAY(claimpayment.Adj_Group_5_Reason_3, Adj_Group_5_Amt_3, Adj_Group_5_Qty_3),
            ARRAY(claimpayment.Adj_Group_5_Reason_4, Adj_Group_5Amt_4, Adj_Group_5_Qty_4),
            ARRAY(claimpayment.Adj_Group_5_Reason_5, Adj_Group_5_Amt_5, Adj_Group_5_Qty_5),
            ARRAY(claimpayment.Adj_Group_5_Reason_6, Adj_Group_5_Amt_6, Adj_Group_5_Qty_6)
        ))[x.AdjGrp_5] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.AdjGrp_5 = 0
    )

