SELECT
    CONCAT('42_', claim.id)                     AS hv_medcl_clm_pymt_sumry_id,
    '005010X221A1'                              AS src_vrsn_id,
    CONCAT('42_', claim.id)                     AS vdr_medcl_clm_pymt_sumry_id,
    'VENDOR'                                    AS vdr_medcl_clm_pymt_sumry_id_qual,
    claim.master_patient_id                     AS hvid,
    extract_date(
        COALESCE(
            claim.claimstartperiod,
            MIN(service.servicedate) OVER (PARTITION BY claim.id)
        ),
        '%Y%m%d'
    )                                           AS clm_stmt_perd_start_dt,
    extract_date(
        COALESCE(
            claim.claimendperiod,
            MAX(service.servicedate) OVER (PARTITION BY claim.id)
        ),
        '%Y%m%d'
    )                                           AS clm_stmt_perd_end_dt,
    extract_date(
        claim.claimreceiveddate,
        '%Y%m%d'
    )                                           AS payr_clm_recpt_dt,
    claim.correctedprioritypayeridentificationcode
                                                AS payr_id,
    claim.correctedprioritypayerentitytypequalifier
                                                AS payr_id_qual,
    claim.payername                             AS payr_nm,
    claim.payeenpid                             AS bllg_prov_npi,
    claim.payeeid                               AS bllg_prov_vdr_id,
    claim.payeeextraid                          AS bllg_prov_tax_id,
    claim.payeename                             AS bllg_prov_1_nm,
    claim.payeeaddressone                       AS bllg_prov_addr_1_txt,
    claim.payeeaddresstwo                       AS bllg_prov_addr_2_txt,
    claim.payeecity                             AS bllg_prov_city_nm,
    claim.payeestate                            AS bllg_prov_state_cd,
    claim.payeezip                              AS bllg_prov_zip_cd,
    claim.remitamount                           AS clm_prov_pymt_amt,
    extract_date(
        claim.remitdatetime,
        '%Y%m%d'
    )                                           AS clm_prov_pymt_dt,
    claim.claimstatuscode                       AS clm_stat_cd,
    claim.totalclaimchargeamount                AS clm_submtd_chg_amt,
    claim.claimpaymentamount                    AS clm_pymt_amt,
    claim.patientresponsibilityamount           AS ptnt_respbty_amt,
    MD5(claim.patientcontrolnumber)             AS ptnt_ctl_num,
    MD5(claim.payerclaimcontrolnumber)          AS payr_clm_ctl_num,
    CASE WHEN claim.frequencycode IS NULL
        THEN claim.facilitytypecode END         AS pos_cd,
    CONCAT(claim.frequencycode, claim.facilitytypecode)
                                                AS instnl_typ_of_bll_cd,
    claim.diagnosisrelatedgroupdrgcode          AS drg_cd,
    claim.diagnosisrelatedgroupdrgweight        AS drg_weight_num,
    claim.dischargefraction                     AS dischg_frctn_num,
    CASE WHEN claim.serviceprovideridentificationcodequalifier = 'XX'
        THEN claim.serviceprovideridentificationcode
    END                                         AS rndrg_prov_npi,
    claim.renderingproviderreferenceidentification
                                                AS rndrg_prov_vdr_id,
    CASE WHEN claim.serviceprovideridentificationcodequalifier = 'FI'
        THEN claim.serviceprovideridentificationcode
    END                                         AS rndrg_prov_tax_id,
    CASE WHEN claim.serviceprovideridentificationcodequalifier = 'FI'
            AND claim.serviceproviderentitytypequalifier = '1'
        THEN claim.serviceprovideridentificationcode
    END                                         AS rndrg_prov_ssn,
    CASE WHEN claim.serviceprovideridentificationcodequalifier = 'SL'
        THEN claim.serviceprovideridentificationcode
    END                                         AS rndrg_prov_state_lic_id,
    CASE WHEN claim.serviceprovideridentificationcodequalifier = 'UP'
        THEN claim.serviceprovideridentificationcode
    END                                         AS rndrg_prov_upin,
    CASE WHEN claim.serviceprovideridentificationcodequalifier = 'PC'
        THEN claim.serviceprovideridentificationcode
    END                                         AS rndrg_prov_comrcl_id,
    claim.serviceproviderlastname               AS rndrg_prov_1_nm,
    claim.serviceproviderfirstname              AS rndrg_prov_2_nm,
    CONCAT_WS(' ', claim.crossovercarrierfirstname, claim.crossovercarrierlastname)
                                                AS cob_pyr_nm,
    claim.crossovercarrierentityidentifiercode  AS cob_pyr_id,
    claim.crossovercarrierentitytypequalifier   AS cob_pyr_id_qual,
    claim.coverdaysorvisitscount                AS covrd_day_vst_cnt,
    claim.ppsoperatingoutlieramount             AS pps_operg_outlr_amt,
    claim.lifetimepsychiatricdayscount          AS lftm_psychtrc_day_cnt,
    claim.claimdrgamount                        AS clm_drg_amt,
    claim.claimdisproportionateshareamount      AS clm_dsh_amt,
    claim.claimmsppassthroughamount             AS clm_msp_pass_thru_amt,
    claim.claimppscapitalamount                 AS clm_pps_captl_amt,
    claim.ppsoperatingfederalspecificdrgamount  AS pps_captl_fsp_drg_amt,
    claim.ppscapitalhspdrgamount                AS pps_captl_hsp_drg_amt,
    claim.ppscapitaldshdrgamount                AS pps_captl_dsh_drg_amt,
    claim.oldcapitalamount                      AS prev_rptg_perd_captl_amt,
    claim.ppscapitalimeamount                   AS pps_captl_ime_amt,
    claim.ppsoperatinghospitalspecificdrgamount AS pps_operg_hsp_drg_amt,
    claim.costreportdaycount                    AS cost_rpt_day_cnt,
    claim.ppscapitalfspdrgamount                AS pps_operg_fsp_drg_amt,
    claim.claimppscapitaloutlieramount          AS clm_pps_captl_outlr_amt,
    claim.claimindirectteachingamount           AS clm_indrct_tchg_amt,
    x.explode_idx + 1                           AS clm_pymt_remrk_cd_seq_num,
    UPPER(
        densify_scalar_array(ARRAY(
            claim.claimpaymentremarkcode,
            claim.claimpaymentremarkcodeone,
            claim.claimpaymentremarkcodetwo,
            claim.claimpaymentremarkcodethree,
            claim.claimpaymentremarkcodefour
        ))[x.explode_idx]
    )                                           AS clm_pymt_remrk_cd,
    claim.ppscapitalexceptionamount             AS pps_captl_excptn_amt,
    x2.explode_idx + 1                          AS clm_amt_seq_num,
    CASE WHEN densify_2d_array(ARRAY(
        ARRAY(claim.coverageamount, COALESCE(claim.coverageamountqualifiercode, 'AU')),
        ARRAY(claim.discountamount, COALESCE(claim.discountamountqualifiercode, 'D8')),
        ARRAY(claim.fedmedcat1amount, COALESCE(claim.fedmedcat1amountqualifiercode, 'ZK')),
        ARRAY(claim.fedmedcat2amount, COALESCE(claim.fedmedcat2amountqualifiercode, 'ZL')),
        ARRAY(claim.fedmedcat3amount, COALESCE(claim.fedmedcat3amountqualifiercode, 'ZM')),
        ARRAY(claim.fedmedcat4amount, COALESCE(claim.fedmedcat4amountqualifiercode, 'ZN')),
        ARRAY(claim.fedmedcat5amount, COALESCE(claim.fedmedcat5amountqualifiercode, 'ZO')),
        ARRAY(claim.interestamount, COALESCE(claim.interestamountqualifiercode, 'I')),
        ARRAY(claim.negativeledgeramount, COALESCE(claim.negativeledgeramountqualifiercode, 'NL')),
        ARRAY(claim.nonpayableprofessionalcomponentamount, NULL),
        ARRAY(claim.perdiemamount, COALESCE(claim.perdiemamountqualifiercode, 'DY')),
        ARRAY(claim.totalclaimbeforetaxesamount, COALESCE(claim.totalclaimbeforetaxesamountqualifiercode, 'T2')),
        ARRAY(claim.patientpaidamount, COALESCE(claim.patientpaidamountqualifiercode, 'F5'))
    ))[x2.explode_idx][1] != 'T'
        THEN densify_2d_array(ARRAY(
            ARRAY(claim.coverageamount, COALESCE(claim.coverageamountqualifiercode, 'AU')),
            ARRAY(claim.discountamount, COALESCE(claim.discountamountqualifiercode, 'D8')),
            ARRAY(claim.fedmedcat1amount, COALESCE(claim.fedmedcat1amountqualifiercode, 'ZK')),
            ARRAY(claim.fedmedcat2amount, COALESCE(claim.fedmedcat2amountqualifiercode, 'ZL')),
            ARRAY(claim.fedmedcat3amount, COALESCE(claim.fedmedcat3amountqualifiercode, 'ZM')),
            ARRAY(claim.fedmedcat4amount, COALESCE(claim.fedmedcat4amountqualifiercode, 'ZN')),
            ARRAY(claim.fedmedcat5amount, COALESCE(claim.fedmedcat5amountqualifiercode, 'ZO')),
            ARRAY(claim.interestamount, COALESCE(claim.interestamountqualifiercode, 'I')),
            ARRAY(claim.negativeledgeramount, COALESCE(claim.negativeledgeramountqualifiercode, 'NL')),
            ARRAY(claim.nonpayableprofessionalcomponentamount, NULL),
            ARRAY(claim.perdiemamount, COALESCE(claim.perdiemamountqualifiercode, 'DY')),
            ARRAY(claim.totalclaimbeforetaxesamount, COALESCE(claim.totalclaimbeforetaxesamountqualifiercode, 'T2')),
            ARRAY(claim.patientpaidamount, COALESCE(claim.patientpaidamountqualifiercode, 'F5'))
        ))[x2.explode_idx][0]
    END                                         AS clm_amt,
    densify_2d_array(ARRAY(
        ARRAY(claim.coverageamount, COALESCE(claim.coverageamountqualifiercode, 'AU')),
        ARRAY(claim.discountamount, COALESCE(claim.discountamountqualifiercode, 'D8')),
        ARRAY(claim.fedmedcat1amount, COALESCE(claim.fedmedcat1amountqualifiercode, 'ZK')),
        ARRAY(claim.fedmedcat2amount, COALESCE(claim.fedmedcat2amountqualifiercode, 'ZL')),
        ARRAY(claim.fedmedcat3amount, COALESCE(claim.fedmedcat3amountqualifiercode, 'ZM')),
        ARRAY(claim.fedmedcat4amount, COALESCE(claim.fedmedcat4amountqualifiercode, 'ZN')),
        ARRAY(claim.fedmedcat5amount, COALESCE(claim.fedmedcat5amountqualifiercode, 'ZO')),
        ARRAY(claim.interestamount, COALESCE(claim.interestamountqualifiercode, 'I')),
        ARRAY(claim.negativeledgeramount, COALESCE(claim.negativeledgeramountqualifiercode, 'NL')),
        ARRAY(claim.nonpayableprofessionalcomponentamount, NULL),
        ARRAY(claim.perdiemamount, COALESCE(claim.perdiemamountqualifiercode, 'DY')),
        ARRAY(claim.totalclaimbeforetaxesamount, COALESCE(claim.totalclaimbeforetaxesamountqualifiercode, 'T2')),
        ARRAY(claim.patientpaidamount, COALESCE(claim.patientpaidamountqualifiercode, 'F5'))
    ))[x2.explode_idx][1]                       AS clm_amt_qual,
    x3.explode_idx + 1                          AS clm_qty_seq_num,
    densify_2d_array(ARRAY(
        ARRAY(claim.coinsuredquantity, claim.coinsuredqualifiercode),
        ARRAY(claim.fedmedcatonequantity, claim.fedmedcatonequantityqualifiercode),
        ARRAY(claim.fedmedcattwoquantity, claim.fedmedcattwoquantityqualifiercode),
        ARRAY(claim.fedmedcatthreequantity, claim.fedmedcatthreequantityqualifiercode),
        ARRAY(claim.fedmedcatfourquantity, claim.fedmedcatfourquantityqualifiercode),
        ARRAY(claim.fedmedcatfivequantity, claim.fedmedcatfivequantityqualifiercode),
        ARRAY(claim.lifetimereservedactualquantity, claim.lifetimereservedactualqualifiercode),
        ARRAY(claim.lifetimereservedestimatedquantity, claim.lifetimereservedestimatedqualifiercode),
        ARRAY(claim.noncoveredquantity, claim.noncoveredqualifiercode),
        ARRAY(claim.notreplacedquantity, claim.notreplacedqualifiercode),
        ARRAY(claim.outlierdaysquantity, claim.outlierdaysqualifiercode),
        ARRAY(claim.prescriptionquantity, claim.prescriptionqualifiercode),
        ARRAY(claim.visitsquantity, claim.visitsqualifiercode)
    ))[x3.explode_idx][0]                        AS clm_qty,
    densify_2d_array(ARRAY(
        ARRAY(claim.coinsuredquantity, claim.coinsuredqualifiercode),
        ARRAY(claim.fedmedcatonequantity, claim.fedmedcatonequantityqualifiercode),
        ARRAY(claim.fedmedcattwoquantity, claim.fedmedcattwoquantityqualifiercode),
        ARRAY(claim.fedmedcatthreequantity, claim.fedmedcatthreequantityqualifiercode),
        ARRAY(claim.fedmedcatfourquantity, claim.fedmedcatfourquantityqualifiercode),
        ARRAY(claim.fedmedcatfivequantity, claim.fedmedcatfivequantityqualifiercode),
        ARRAY(claim.lifetimereservedactualquantity, claim.lifetimereservedactualqualifiercode),
        ARRAY(claim.lifetimereservedestimatedquantity, claim.lifetimereservedestimatedqualifiercode),
        ARRAY(claim.noncoveredquantity, claim.noncoveredqualifiercode),
        ARRAY(claim.notreplacedquantity, claim.notreplacedqualifiercode),
        ARRAY(claim.outlierdaysquantity, claim.outlierdaysqualifiercode),
        ARRAY(claim.prescriptionquantity, claim.prescriptionqualifiercode),
        ARRAY(claim.visitsquantity, claim.visitsqualifiercode)
    ))[x3.explode_idx][1]                       AS clm_qty_qual,
    x4.explode_idx + 1                          AS clm_adjmt_seq_num,
    adjust.adjustmentgroupcode                  AS clm_adjmt_grp_cd,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x4.explode_idx][0]                       AS clm_adjmt_rsn_txt,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x4.explode_idx][1]                       AS clm_adjmt_amt,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x4.explode_idx][2]                       AS clm_adjmt_qty,
    claim.groupcontrolnumber                    AS clm_fnctnl_grp_ctl_num
FROM
    remit_claim claim
    LEFT JOIN
        remit_claim_adjustment adjust
            ON claim.id = adjust.remitclaim_id
    LEFT JOIN
        serviceline service
            ON claim.id = service.remitclaim_id
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4)) as explode_idx) x
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) as explode_idx) x2
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) as explode_idx) x3
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5)) as explode_idx) x4
    WHERE 
        (densify_scalar_array(ARRAY(
            claim.claimpaymentremarkcode,
            claim.claimpaymentremarkcodeone,
            claim.claimpaymentremarkcodetwo,
            claim.claimpaymentremarkcodethree,
            claim.claimpaymentremarkcodefour
        ))[x.explode_idx] != NULL OR x.explode_idx = 0)
        AND
        (densify_2d_array(ARRAY(
            ARRAY(claim.coverageamount, COALESCE(claim.coverageamountqualifiercode, 'AU')),
            ARRAY(claim.discountamount, COALESCE(claim.discountamountqualifiercode, 'D8')),
            ARRAY(claim.fedmedcat1amount, COALESCE(claim.fedmedcat1amountqualifiercode, 'ZK')),
            ARRAY(claim.fedmedcat2amount, COALESCE(claim.fedmedcat2amountqualifiercode, 'ZL')),
            ARRAY(claim.fedmedcat3amount, COALESCE(claim.fedmedcat3amountqualifiercode, 'ZM')),
            ARRAY(claim.fedmedcat4amount, COALESCE(claim.fedmedcat4amountqualifiercode, 'ZN')),
            ARRAY(claim.fedmedcat5amount, COALESCE(claim.fedmedcat5amountqualifiercode, 'ZO')),
            ARRAY(claim.interestamount, COALESCE(claim.interestamountqualifiercode, 'I')),
            ARRAY(claim.negativeledgeramount, COALESCE(claim.negativeledgeramountqualifiercode, 'NL')),
            ARRAY(claim.nonpayableprofessionalcomponentamount, NULL),
            ARRAY(claim.perdiemamount, COALESCE(claim.perdiemamountqualifiercode, 'DY')),
            ARRAY(claim.totalclaimbeforetaxesamount, COALESCE(claim.totalclaimbeforetaxesamountqualifiercode, 'T2')),
            ARRAY(claim.patientpaidamount, COALESCE(claim.patientpaidamountqualifiercode, 'F5'))
        ))[x2.explode_idx] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x2.explode_idx = 0)
        AND
        (densify_2d_array(ARRAY(
            ARRAY(claim.coinsuredquantity, claim.coinsuredqualifiercode),
            ARRAY(claim.fedmedcatonequantity, claim.fedmedcatonequantityqualifiercode),
            ARRAY(claim.fedmedcattwoquantity, claim.fedmedcattwoquantityqualifiercode),
            ARRAY(claim.fedmedcatthreequantity, claim.fedmedcatthreequantityqualifiercode),
            ARRAY(claim.fedmedcatfourquantity, claim.fedmedcatfourquantityqualifiercode),
            ARRAY(claim.fedmedcatfivequantity, claim.fedmedcatfivequantityqualifiercode),
            ARRAY(claim.lifetimereservedactualquantity, claim.lifetimereservedactualqualifiercode),
            ARRAY(claim.lifetimereservedestimatedquantity, claim.lifetimereservedestimatedqualifiercode),
            ARRAY(claim.noncoveredquantity, claim.noncoveredqualifiercode),
            ARRAY(claim.notreplacedquantity, claim.notreplacedqualifiercode),
            ARRAY(claim.outlierdaysquantity, claim.outlierdaysqualifiercode),
            ARRAY(claim.prescriptionquantity, claim.prescriptionqualifiercode),
            ARRAY(claim.visitsquantity, claim.visitsqualifiercode)
        ))[x3.explode_idx] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x3.explode_idx = 0)
        AND
        (densify_2d_array(ARRAY(
            ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
            ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
            ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
            ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
            ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
            ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
        ))[x4.explode_idx] != CAST(ARRAY(NULL, NULL, NULL) AS ARRAY<STRING>) OR x4.explode_idx = 0)
