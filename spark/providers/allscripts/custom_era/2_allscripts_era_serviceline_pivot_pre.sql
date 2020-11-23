SELECT 
    serviceline.Service_Payemnt_Indicator,
    header.Batch_ID,
    serviceline.Claim_ID,
    serviceline.Sequence_Number,
    serviceline.Product_or_Service_ID_Qualifier,
    serviceline.Adjudicated_Procedure_Code,
    serviceline.Adjudicated_Procedure_Modifier_1,
    serviceline.Adjudicated_Procedure_Modifier_2,
    serviceline.Adjudicated_Procedure_Modifer_3,
    serviceline.Adjudicated_Procedure_Modifier_4,
    serviceline.Procedure_Code_Description,
    serviceline.Line_Item_Charge_Amount,
    serviceline.Line_Item_Provider_Payment_Amount,
    serviceline.NUBC_Revenue_Code,
    serviceline.Units_of_Service_Paid_Count,
    serviceline.Original_Product_or_Service_ID_Qualifier,
    serviceline.Original_Submitted_Procedure_Code,
    serviceline.Original_Submitted_Procedure_Modifiers_1,
    serviceline.Original_Submitted_Procedure_Modifiers_2,
    serviceline.Original_Submitted_Procedure_Modifiers_3,
    serviceline.Original_Submitted_Procedure_Modifiers_4,
    serviceline.Original_Units_of_Service_Count,
    serviceline.Date_Time_Qualifier,
    serviceline.Service_From_Date,
    serviceline.Service_To_Date,
    serviceline.Service_ID_Qualifier_1S,
    serviceline.Ambulatory_Patient_Group_Number,
    serviceline.Service_ID_Qualifier_APG,
    serviceline.Ambulatory_Payment_Classification,
    serviceline.Service_ID_Qualifier_BB,
    serviceline.Authorization_Number,
    serviceline.Service_ID_Qualifier_E9,
    serviceline.Attachment_Code,
    serviceline.Service_ID_Qualifier_G1,
    serviceline.Prior_Authorization_Number,
    serviceline.Service_ID_Qualifier_G3,
    serviceline.Predetermination_of_Benefits_ID_Number,
    serviceline.Service_ID_Qualifier_LU,
    serviceline.Location_Number,
    serviceline.Service_ID_Qualifier_RB,
    serviceline.Rate_Code_Number,
    serviceline.Line_Item_Control_Number___,
    serviceline.Rendering_Provider_ID_Qual_1_,
    serviceline.Rendering_Provider_ID__1,
    serviceline.Rendering_Provider_ID_Qual_2,
    serviceline.Rendering_Provider_ID_2,
    serviceline.Rendering_Provider_ID_Qual_3,
    serviceline.Rendering_Provider_ID__3,
    serviceline.Rendering_Provider_ID_Qual_4,
    serviceline.Rendering_Provider_ID__4,
    serviceline.Rendering_Provider_ID_Qual_5,
    serviceline.Rendering_Provider_ID__5,
    serviceline.Rendering_Provider_ID_Qual_6,
    serviceline.Rendering_Provider_ID__6,
    serviceline.Rendering_Provider_ID_Qual_7,
    serviceline.Rendering_Provider_ID__7,
    serviceline.Rendering_Provider_ID_Qual_8,
    serviceline.Rendering_Provider_ID__8,
    serviceline.Rendering_Provider_ID_Qual_9,
    serviceline.Rendering_Provider_ID__9,
    serviceline.Rendering_Provider_ID_Qual_10,
    serviceline.Rendering_Provider_ID_10,
    serviceline.Healthcare_Policy_ID_Qual_1,
    serviceline.Healthcare_Policy_ID__1,
    serviceline.Healthcare_Policy_ID_Qual_2,
    serviceline.Healthcare_Policy_ID__2,
    serviceline.Healthcare_Policy_ID_Qual_3,
    serviceline.Healthcare_Policy_ID__3,
    serviceline.Healthcare_Policy_ID_Qual_4,
    serviceline.Healthcare_Policy_ID__4,
    serviceline.Healthcare_Policy_ID_Qual_5,
    serviceline.Healthcare_Policy_ID__5,
    serviceline.Service_Line_Supplemental_Amount_Identifier_Informational,
    serviceline.Supplemental_Service_Amount,
    serviceline.Allowed_Amount,
    serviceline.Late_Amount,
    serviceline.Service_Line_Supplemental_Quantity_Identifier_Informational,
    serviceline.Estimated_Non_Covered,
    serviceline.Service_Line_Supplemental_Quantity_Identifier_Informational_1,
    serviceline.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_1,
    serviceline.Service_Line_Supplemental_Quantity_Identifier_Informational_2,
    serviceline.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_2,
    serviceline.Service_Line_Supplemental_Quantity_Identifier_Informational_3,
    serviceline.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_3,
    serviceline.Service_Line_Supplemental_Quantity_Identifier_Informational_4,
    serviceline.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_4,
    serviceline.Service_Line_Supplemental_Quantity_Identifier_Informational_5,
    serviceline.Federal_Medicare_or_Medicaid_Payment_Mandate_Category_5,
    serviceline.part_processdate,
    CASE WHEN serviceline.Svc_Line_Adjustment_Group_Code_1 IS NOT NULL
    THEN 1
    ELSE NULL END                                         AS svcpvt_svc_ln_adjmt_seq_num_1, 
    serviceline.Svc_Line_Adjustment_Group_Code_1          AS svcpvt_svc_ln_adjmt_grp_cd_1,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_1_Reason_1, serviceline.Svc_Line_Adj_Group_1_Amt_1, serviceline.Svc_Line_Adj_Group_1_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_2, serviceline.Svc_Line_Adj_Group_1_Amt_2, serviceline.Svc_Line_Adj_Group_1_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_3, serviceline.Svc_Line_Adj_Group_1_Amt_3, serviceline.Svc_Line_Adj_Group_1_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_4, serviceline.Svc_Line_Adj_Group_1_Amt_4, serviceline.Svc_Line_Adj_Group_1_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_5, serviceline.Svc_Line_Adj_Group_1_Amt_5, serviceline.Svc_Line_Adj_Group_1_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_6, serviceline.Svc_Line_Adj_Group_1_Amt_6, serviceline.Svc_Line_Adj_Group_1_Qty_6)
    ))[x.SvcAdjGrp_1][0]                               AS svcpvt_svc_ln_adjmt_rsn_cd_1,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_1_Reason_1, serviceline.Svc_Line_Adj_Group_1_Amt_1, serviceline.Svc_Line_Adj_Group_1_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_2, serviceline.Svc_Line_Adj_Group_1_Amt_2, serviceline.Svc_Line_Adj_Group_1_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_3, serviceline.Svc_Line_Adj_Group_1_Amt_3, serviceline.Svc_Line_Adj_Group_1_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_4, serviceline.Svc_Line_Adj_Group_1_Amt_4, serviceline.Svc_Line_Adj_Group_1_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_5, serviceline.Svc_Line_Adj_Group_1_Amt_5, serviceline.Svc_Line_Adj_Group_1_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_6, serviceline.Svc_Line_Adj_Group_1_Amt_6, serviceline.Svc_Line_Adj_Group_1_Qty_6)
    ))[x.SvcAdjGrp_1][1]                                AS svcpvt_svc_ln_adjmt_amt_1,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_1_Reason_1, serviceline.Svc_Line_Adj_Group_1_Amt_1, serviceline.Svc_Line_Adj_Group_1_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_2, serviceline.Svc_Line_Adj_Group_1_Amt_2, serviceline.Svc_Line_Adj_Group_1_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_3, serviceline.Svc_Line_Adj_Group_1_Amt_3, serviceline.Svc_Line_Adj_Group_1_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_4, serviceline.Svc_Line_Adj_Group_1_Amt_4, serviceline.Svc_Line_Adj_Group_1_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_5, serviceline.Svc_Line_Adj_Group_1_Amt_5, serviceline.Svc_Line_Adj_Group_1_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_6, serviceline.Svc_Line_Adj_Group_1_Amt_6, serviceline.Svc_Line_Adj_Group_1_Qty_6)
    ))[x.SvcAdjGrp_1][2]                                AS svcpvt_svc_ln_adjmt_qty_1,
    CASE WHEN serviceline.Svc_Line_Adjustment_Group_Code_2 IS NOT NULL
    THEN 2
    ELSE NULL END                                         AS svcpvt_svc_ln_adjmt_seq_num_2, 
    serviceline.Svc_Line_Adjustment_Group_Code_2          AS svcpvt_svc_ln_adjmt_grp_cd_2,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_2_Reason_1, serviceline.Svc_Line_Adj_Group_2_Amt_1, serviceline.Svc_Line_Adj_Group_2_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_2, serviceline.Svc_Line_Adj_Group_2_Amt_2, serviceline.Svc_Line_Adj_Group_2_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_3, serviceline.Svc_Line_Adj_Group_2_Amt_3, serviceline.Svc_Line_Adj_Group_2_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_4, serviceline.Svc_Line_Adj_Group_2_Amt_4, serviceline.Svc_Line_Adj_Group_2_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_5, serviceline.Svc_Line_Adj_Group_2_Amt_5, serviceline.Svc_Line_Adj_Group_2_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_6, serviceline.Svc_Line_Adj_Group_2_Amt_6, serviceline.Svc_Line_Adj_Group_2_Qty_6)
    ))[x.SvcAdjGrp_2][0]                               AS svcpvt_svc_ln_adjmt_rsn_cd_2,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_2_Reason_1, serviceline.Svc_Line_Adj_Group_2_Amt_1, serviceline.Svc_Line_Adj_Group_2_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_2, serviceline.Svc_Line_Adj_Group_2_Amt_2, serviceline.Svc_Line_Adj_Group_2_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_3, serviceline.Svc_Line_Adj_Group_2_Amt_3, serviceline.Svc_Line_Adj_Group_2_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_4, serviceline.Svc_Line_Adj_Group_2_Amt_4, serviceline.Svc_Line_Adj_Group_2_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_5, serviceline.Svc_Line_Adj_Group_2_Amt_5, serviceline.Svc_Line_Adj_Group_2_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_6, serviceline.Svc_Line_Adj_Group_2_Amt_6, serviceline.Svc_Line_Adj_Group_2_Qty_6)
    ))[x.SvcAdjGrp_2][1]                                AS svcpvt_svc_ln_adjmt_amt_2,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_2_Reason_1, serviceline.Svc_Line_Adj_Group_2_Amt_1, serviceline.Svc_Line_Adj_Group_2_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_2, serviceline.Svc_Line_Adj_Group_2_Amt_2, serviceline.Svc_Line_Adj_Group_2_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_3, serviceline.Svc_Line_Adj_Group_2_Amt_3, serviceline.Svc_Line_Adj_Group_2_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_4, serviceline.Svc_Line_Adj_Group_2_Amt_4, serviceline.Svc_Line_Adj_Group_2_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_5, serviceline.Svc_Line_Adj_Group_2_Amt_5, serviceline.Svc_Line_Adj_Group_2_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_6, serviceline.Svc_Line_Adj_Group_2_Amt_6, serviceline.Svc_Line_Adj_Group_2_Qty_6)
    ))[x.SvcAdjGrp_2][2]                                AS svcpvt_svc_ln_adjmt_qty_2,
    CASE WHEN serviceline.Svc_Line_Adjustment_Group_Code_3 IS NOT NULL
    THEN 3
    ELSE NULL END                                         AS svcpvt_svc_ln_adjmt_seq_num_3, 
    serviceline.Svc_Line_Adjustment_Group_Code_3          AS svcpvt_svc_ln_adjmt_grp_cd_3,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_3_Reason_1, serviceline.Svc_Line_Adj_Group_3_Amt_1, serviceline.Svc_Line_Adj_Group_3_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_2, serviceline.Svc_Line_Adj_Group_3_Amt_2, serviceline.Svc_Line_Adj_Group_3_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_3, serviceline.Svc_Line_Adj_Group_3_Amt_3, serviceline.Svc_Line_Adj_Group_3_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_4, serviceline.Svc_Line_Adj_Group_3_Amt_4, serviceline.Svc_Line_Adj_Group_3_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_5, serviceline.Svc_Line_Adj_Group_3_Amt_5, serviceline.Svc_Line_Adj_Group_3_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_6, serviceline.Svc_Line_Adj_Group_3_Amt_6, serviceline.Svc_Line_Adj_Group_3_Qty_6)
    ))[x.SvcAdjGrp_3][0]                               AS svcpvt_svc_ln_adjmt_rsn_cd_3,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_3_Reason_1, serviceline.Svc_Line_Adj_Group_3_Amt_1, serviceline.Svc_Line_Adj_Group_3_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_2, serviceline.Svc_Line_Adj_Group_3_Amt_2, serviceline.Svc_Line_Adj_Group_3_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_3, serviceline.Svc_Line_Adj_Group_3_Amt_3, serviceline.Svc_Line_Adj_Group_3_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_4, serviceline.Svc_Line_Adj_Group_3_Amt_4, serviceline.Svc_Line_Adj_Group_3_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_5, serviceline.Svc_Line_Adj_Group_3_Amt_5, serviceline.Svc_Line_Adj_Group_3_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_6, serviceline.Svc_Line_Adj_Group_3_Amt_6, serviceline.Svc_Line_Adj_Group_3_Qty_6)
    ))[x.SvcAdjGrp_3][1]                                AS svcpvt_svc_ln_adjmt_amt_3,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_3_Reason_1, serviceline.Svc_Line_Adj_Group_3_Amt_1, serviceline.Svc_Line_Adj_Group_3_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_2, serviceline.Svc_Line_Adj_Group_3_Amt_2, serviceline.Svc_Line_Adj_Group_3_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_3, serviceline.Svc_Line_Adj_Group_3_Amt_3, serviceline.Svc_Line_Adj_Group_3_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_4, serviceline.Svc_Line_Adj_Group_3_Amt_4, serviceline.Svc_Line_Adj_Group_3_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_5, serviceline.Svc_Line_Adj_Group_3_Amt_5, serviceline.Svc_Line_Adj_Group_3_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_6, serviceline.Svc_Line_Adj_Group_3_Amt_6, serviceline.Svc_Line_Adj_Group_3_Qty_6)
    ))[x.SvcAdjGrp_3][2]                                AS svcpvt_svc_ln_adjmt_qty_3,
    CASE WHEN serviceline.Svc_Line_Adjustment_Group_Code_4 IS NOT NULL
    THEN 4
    ELSE NULL END                                         AS svcpvt_svc_ln_adjmt_seq_num_4, 
    serviceline.Svc_Line_Adjustment_Group_Code_4          AS svcpvt_svc_ln_adjmt_grp_cd_4,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_4_Reason_1, serviceline.Svc_Line_Adj_Group_4_Amt_1, serviceline.Svc_Line_Adj_Group_4_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_2, serviceline.Svc_Line_Adj_Group_4_Amt_2, serviceline.Svc_Line_Adj_Group_4_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_3, serviceline.Svc_Line_Adj_Group_4_Amt_3, serviceline.Svc_Line_Adj_Group_4_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_4, serviceline.Svc_Line_Adj_Group_4_Amt_4, serviceline.Svc_Line_Adj_Group_4_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_5, serviceline.Svc_Line_Adj_Group_4_Amt_5, serviceline.Svc_Line_Adj_Group_4_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_6, serviceline.Svc_Line_Adj_Group_4_Amt_6, serviceline.Svc_Line_Adj_Group_4_Qty_6)
    ))[x.SvcAdjGrp_4][0]                               AS svcpvt_svc_ln_adjmt_rsn_cd_4,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_4_Reason_1, serviceline.Svc_Line_Adj_Group_4_Amt_1, serviceline.Svc_Line_Adj_Group_4_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_2, serviceline.Svc_Line_Adj_Group_4_Amt_2, serviceline.Svc_Line_Adj_Group_4_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_3, serviceline.Svc_Line_Adj_Group_4_Amt_3, serviceline.Svc_Line_Adj_Group_4_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_4, serviceline.Svc_Line_Adj_Group_4_Amt_4, serviceline.Svc_Line_Adj_Group_4_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_5, serviceline.Svc_Line_Adj_Group_4_Amt_5, serviceline.Svc_Line_Adj_Group_4_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_6, serviceline.Svc_Line_Adj_Group_4_Amt_6, serviceline.Svc_Line_Adj_Group_4_Qty_6)
    ))[x.SvcAdjGrp_4][1]                                AS svcpvt_svc_ln_adjmt_amt_4,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_4_Reason_1, serviceline.Svc_Line_Adj_Group_4_Amt_1, serviceline.Svc_Line_Adj_Group_4_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_2, serviceline.Svc_Line_Adj_Group_4_Amt_2, serviceline.Svc_Line_Adj_Group_4_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_3, serviceline.Svc_Line_Adj_Group_4_Amt_3, serviceline.Svc_Line_Adj_Group_4_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_4, serviceline.Svc_Line_Adj_Group_4_Amt_4, serviceline.Svc_Line_Adj_Group_4_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_5, serviceline.Svc_Line_Adj_Group_4_Amt_5, serviceline.Svc_Line_Adj_Group_4_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_6, serviceline.Svc_Line_Adj_Group_4_Amt_6, serviceline.Svc_Line_Adj_Group_4_Qty_6)
    ))[x.SvcAdjGrp_4][2]                                AS svcpvt_svc_ln_adjmt_qty_4,
    CASE WHEN serviceline.Svc_Line_Adjustment_Group_Code_5 IS NOT NULL
    THEN 5
    ELSE NULL END                                         AS svcpvt_svc_ln_adjmt_seq_num_5, 
    serviceline.Svc_Line_Adjustment_Group_Code_5          AS svcpvt_svc_ln_adjmt_grp_cd_5,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_5_Reason_1, serviceline.Svc_Line_Adj_Group_5_Amt_1, serviceline.Svc_Line_Adj_Group_5_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_2, serviceline.Svc_Line_Adj_Group_5_Amt_2, serviceline.Svc_Line_Adj_Group_5_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_3, serviceline.Svc_Line_Adj_Group_5_Amt_3, serviceline.Svc_Line_Adj_Group_5_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_4, serviceline.Svc_Line_Adj_Group_5_Amt_4, serviceline.Svc_Line_Adj_Group_5_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_5, serviceline.Svc_Line_Adj_Group_5_Amt_5, serviceline.Svc_Line_Adj_Group_5_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_6, serviceline.Svc_Line_Adj_Group_5_Amt_6, serviceline.Svc_Line_Adj_Group_5_Qty_6)
    ))[x.SvcAdjGrp_5][0]                               AS svcpvt_svc_ln_adjmt_rsn_cd_5,
    densify_2d_array(ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_5_Reason_1, serviceline.Svc_Line_Adj_Group_5_Amt_1, serviceline.Svc_Line_Adj_Group_5_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_2, serviceline.Svc_Line_Adj_Group_5_Amt_2, serviceline.Svc_Line_Adj_Group_5_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_3, serviceline.Svc_Line_Adj_Group_5_Amt_3, serviceline.Svc_Line_Adj_Group_5_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_4, serviceline.Svc_Line_Adj_Group_5_Amt_4, serviceline.Svc_Line_Adj_Group_5_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_5, serviceline.Svc_Line_Adj_Group_5_Amt_5, serviceline.Svc_Line_Adj_Group_5_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_6, serviceline.Svc_Line_Adj_Group_5_Amt_6, serviceline.Svc_Line_Adj_Group_5_Qty_6)
    ))[x.SvcAdjGrp_5][1]                                AS svcpvt_svc_ln_adjmt_amt_5,
    densify_2d_array(ARRAY(
             ARRAY(serviceline.Svc_LineAdj_Group_5_Reason_1, serviceline.Svc_Line_Adj_Group_5_Amt_1, serviceline.Svc_Line_Adj_Group_5_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_2, serviceline.Svc_Line_Adj_Group_5_Amt_2, serviceline.Svc_Line_Adj_Group_5_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_3, serviceline.Svc_Line_Adj_Group_5_Amt_3, serviceline.Svc_Line_Adj_Group_5_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_4, serviceline.Svc_Line_Adj_Group_5_Amt_4, serviceline.Svc_Line_Adj_Group_5_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_5, serviceline.Svc_Line_Adj_Group_5_Amt_5, serviceline.Svc_Line_Adj_Group_5_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_6, serviceline.Svc_Line_Adj_Group_5_Amt_6, serviceline.Svc_Line_Adj_Group_5_Qty_6)
    ))[x.SvcAdjGrp_5][2]                                AS svcpvt_svc_ln_adjmt_qty_5,
   densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Supplemental_Service_Amount,ServiceLine.Service_Line_Supplemental_Amount_Identifier_Informational),
            ARRAY(ServiceLine.Allowed_Amount, 'B6'),
            ARRAY(ServiceLine.Late_Amount, 'KH')
    ))[x.SuplmtlAmt][0]                                AS svcpvt_svc_ln_suplmtl_amt,
   densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Supplemental_Service_Amount,ServiceLine.Service_Line_Supplemental_Amount_Identifier_Informational),
            ARRAY(ServiceLine.Allowed_Amount, 'B6'),
            ARRAY(ServiceLine.Late_Amount, 'KH')
    ))[x.SuplmtlAmt][1]                                AS svcpvt_svc_ln_suplmtl_amt_qual,
   CASE WHEN
   densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Remark_Code_1, ServiceLine.Remark_Qual_1),
            ARRAY(ServiceLine.Remark_Code_2, ServiceLine.Remark_Qual_2),
            ARRAY(ServiceLine.Remark_Code_3, ServiceLine.Remark_Qual_3),
            ARRAY(ServiceLine.Remark_Code_4, ServiceLine.Remark_Qual_4),
            ARRAY(ServiceLine.Remark_Code_5, ServiceLine.Remark_Qual_5)
    ))[x.SvcRemarkCd][0] IS NULL 
    THEN NULL 
    ELSE x.SvcRemarkCd + 1 END                          AS svcpvt_svc_ln_rmrk_cd_seq_num, 
   densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Remark_Code_1, ServiceLine.Remark_Qual_1),
            ARRAY(ServiceLine.Remark_Code_2, ServiceLine.Remark_Qual_2),
            ARRAY(ServiceLine.Remark_Code_3, ServiceLine.Remark_Qual_3),
            ARRAY(ServiceLine.Remark_Code_4, ServiceLine.Remark_Qual_4),
            ARRAY(ServiceLine.Remark_Code_5, ServiceLine.Remark_Qual_5)
    ))[x.SvcRemarkCd][0]                                AS svcpvt_svc_ln_remrk_cd,    
   densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Remark_Code_1, ServiceLine.Remark_Qual_1),
            ARRAY(ServiceLine.Remark_Code_2, ServiceLine.Remark_Qual_2),
            ARRAY(ServiceLine.Remark_Code_3, ServiceLine.Remark_Qual_3),
            ARRAY(ServiceLine.Remark_Code_4, ServiceLine.Remark_Qual_4),
            ARRAY(ServiceLine.Remark_Code_5, ServiceLine.Remark_Qual_5)
    ))[x.SvcRemarkCd][1]                                AS svcpvt_svc_ln_remrk_cd_qual   
FROM hdr header
  LEFT OUTER JOIN svc serviceline  
  ON header.batch_id = serviceline.batch_id
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS SvcAdjGrp_1) x
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS SvcAdjGrp_2) x
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS SvcAdjGrp_3) x
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS SvcAdjGrp_4) x
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS SvcAdjGrp_5) x
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2)) AS SuplmtlAmt) x
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS SvcRemarkCd) x

WHERE header.Header_Indicator='HDR' 
----------- Adjustment Group 1 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_1_Reason_1, serviceline.Svc_Line_Adj_Group_1_Amt_1, serviceline.Svc_Line_Adj_Group_1_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_2, serviceline.Svc_Line_Adj_Group_1_Amt_2, serviceline.Svc_Line_Adj_Group_1_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_3, serviceline.Svc_Line_Adj_Group_1_Amt_3, serviceline.Svc_Line_Adj_Group_1_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_4, serviceline.Svc_Line_Adj_Group_1_Amt_4, serviceline.Svc_Line_Adj_Group_1_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_5, serviceline.Svc_Line_Adj_Group_1_Amt_5, serviceline.Svc_Line_Adj_Group_1_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_1_Reason_6, serviceline.Svc_Line_Adj_Group_1_Amt_6, serviceline.Svc_Line_Adj_Group_1_Qty_6)
        ))[x.SvcAdjGrp_1] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.SvcAdjGrp_1 = 0
    )
----------- Adjustment Group 2 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_2_Reason_1, serviceline.Svc_Line_Adj_Group_2_Amt_1, serviceline.Svc_Line_Adj_Group_2_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_2, serviceline.Svc_Line_Adj_Group_2_Amt_2, serviceline.Svc_Line_Adj_Group_2_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_3, serviceline.Svc_Line_Adj_Group_2_Amt_3, serviceline.Svc_Line_Adj_Group_2_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_4, serviceline.Svc_Line_Adj_Group_2_Amt_4, serviceline.Svc_Line_Adj_Group_2_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_5, serviceline.Svc_Line_Adj_Group_2_Amt_5, serviceline.Svc_Line_Adj_Group_2_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_2_Reason_6, serviceline.Svc_Line_Adj_Group_2_Amt_6, serviceline.Svc_Line_Adj_Group_2_Qty_6)
        ))[x.SvcAdjGrp_2] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.SvcAdjGrp_2 = 0
    )
----------- Adjustment Group 3 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_3_Reason_1, serviceline.Svc_Line_Adj_Group_3_Amt_1, serviceline.Svc_Line_Adj_Group_3_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_2, serviceline.Svc_Line_Adj_Group_3_Amt_2, serviceline.Svc_Line_Adj_Group_3_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_3, serviceline.Svc_Line_Adj_Group_3_Amt_3, serviceline.Svc_Line_Adj_Group_3_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_4, serviceline.Svc_Line_Adj_Group_3_Amt_4, serviceline.Svc_Line_Adj_Group_3_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_5, serviceline.Svc_Line_Adj_Group_3_Amt_5, serviceline.Svc_Line_Adj_Group_3_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_3_Reason_6, serviceline.Svc_Line_Adj_Group_3_Amt_6, serviceline.Svc_Line_Adj_Group_3_Qty_6)
        ))[x.SvcAdjGrp_3] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.SvcAdjGrp_3 = 0
    )
----------- Adjustment Group 4 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_4_Reason_1, serviceline.Svc_Line_Adj_Group_4_Amt_1, serviceline.Svc_Line_Adj_Group_4_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_2, serviceline.Svc_Line_Adj_Group_4_Amt_2, serviceline.Svc_Line_Adj_Group_4_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_3, serviceline.Svc_Line_Adj_Group_4_Amt_3, serviceline.Svc_Line_Adj_Group_4_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_4, serviceline.Svc_Line_Adj_Group_4_Amt_4, serviceline.Svc_Line_Adj_Group_4_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_5, serviceline.Svc_Line_Adj_Group_4_Amt_5, serviceline.Svc_Line_Adj_Group_4_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_4_Reason_6, serviceline.Svc_Line_Adj_Group_4_Amt_6, serviceline.Svc_Line_Adj_Group_4_Qty_6)
        ))[x.SvcAdjGrp_4] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.SvcAdjGrp_4 = 0
    )
----------- Adjustment Group 5 : Reason 1 thru 6 , Amount 1 thru 6 and qty 1 thru 6 Explosion
AND
    (densify_2d_array
      (ARRAY(
            ARRAY(serviceline.Svc_LineAdj_Group_5_Reason_1, serviceline.Svc_Line_Adj_Group_5_Amt_1, serviceline.Svc_Line_Adj_Group_5_Qty_1),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_2, serviceline.Svc_Line_Adj_Group_5_Amt_2, serviceline.Svc_Line_Adj_Group_5_Qty_2),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_3, serviceline.Svc_Line_Adj_Group_5_Amt_3, serviceline.Svc_Line_Adj_Group_5_Qty_3),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_4, serviceline.Svc_Line_Adj_Group_5_Amt_4, serviceline.Svc_Line_Adj_Group_5_Qty_4),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_5, serviceline.Svc_Line_Adj_Group_5_Amt_5, serviceline.Svc_Line_Adj_Group_5_Qty_5),
            ARRAY(serviceline.Svc_Line_Adj_Group_5_Reason_6, serviceline.Svc_Line_Adj_Group_5_Amt_6, serviceline.Svc_Line_Adj_Group_5_Qty_6)
        ))[x.SvcAdjGrp_5] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.SvcAdjGrp_5 = 0
    )
----------- Service Line Supplemental Amount Explosion
AND
    (densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Supplemental_Service_Amount,ServiceLine.Service_Line_Supplemental_Amount_Identifier_Informational),
            ARRAY(ServiceLine.Allowed_Amount, 'B6'),
            ARRAY(ServiceLine.Late_Amount, 'KH')
        ))[x.SuplmtlAmt] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x.SuplmtlAmt = 0
    )
----------- Service Line Supplemental Amount Explosion
AND
    (densify_2d_array_by_key
      (ARRAY(
            ARRAY(ServiceLine.Remark_Code_1, ServiceLine.Remark_Qual_1),
            ARRAY(ServiceLine.Remark_Code_2, ServiceLine.Remark_Qual_2),
            ARRAY(ServiceLine.Remark_Code_3, ServiceLine.Remark_Qual_3),
            ARRAY(ServiceLine.Remark_Code_4, ServiceLine.Remark_Qual_4),
            ARRAY(ServiceLine.Remark_Code_5, ServiceLine.Remark_Qual_5)
        ))[x.SvcRemarkCd] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x.SvcRemarkCd = 0
    )
