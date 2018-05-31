SELECT
    CONCAT('42_', the_claim_id)                 AS hv_medcl_clm_pymt_sumry_id,
    '005010X221A1'                              AS src_vrsn_id,
    CONCAT('42_', the_claim_id)                 AS vdr_medcl_clm_pymt_sumry_id,
    'VENDOR'                                    AS vdr_medcl_clm_pymt_sumry_id_qual,
    master_patient_id                           AS hvid,
    extract_date(
        COALESCE(
            claimstartperiod,
            MIN(servicedate) OVER (PARTITION BY the_claim_id)
        ),
        '%Y%m%d'
    )                                           AS clm_stmt_perd_start_dt,
    extract_date(
        COALESCE(
            claimendperiod,
            MAX(servicedate) OVER (PARTITION BY the_claim_id)
        ),
        '%Y%m%d'
    )                                           AS clm_stmt_perd_end_dt,
    extract_date(
        claimreceiveddate,
        '%Y%m%d'
    )                                           AS payr_clm_recpt_dt,
    correctedprioritypayeridentificationcode
                                                AS payr_id,
    correctedprioritypayerentitytypequalifier
                                                AS payr_id_qual,
    payername                                   AS payr_nm,
    payeenpid                                   AS bllg_prov_npi,
    payeeid                                     AS bllg_prov_vdr_id,
    payeeextraid                                AS bllg_prov_tax_id,
    payeename                                   AS bllg_prov_1_nm,
    payeeaddressone                             AS bllg_prov_addr_1_txt,
    payeeaddresstwo                             AS bllg_prov_addr_2_txt,
    payeecity                                   AS bllg_prov_city_nm,
    payeestate                                  AS bllg_prov_state_cd,
    payeezip                                    AS bllg_prov_zip_cd,
    remitamount                                 AS clm_prov_pymt_amt,
    extract_date(
        remitdatetime,
        '%Y%m%d'
    )                                           AS clm_prov_pymt_dt,
    claimstatuscode                             AS clm_stat_cd,
    totalclaimchargeamount                      AS clm_submtd_chg_amt,
    claimpaymentamount                          AS clm_pymt_amt,
    patientresponsibilityamount                 AS ptnt_respbty_amt,
    MD5(patientcontrolnumber)                   AS ptnt_ctl_num,
    MD5(payerclaimcontrolnumber)                AS payr_clm_ctl_num,
    CASE WHEN frequencycode IS NULL
        THEN facilitytypecode END         AS pos_cd,
    CONCAT(frequencycode, facilitytypecode)
                                                AS instnl_typ_of_bll_cd,
    diagnosisrelatedgroupdrgcode                AS drg_cd,
    diagnosisrelatedgroupdrgweight              AS drg_weight_num,
    dischargefraction                           AS dischg_frctn_num,
    CASE WHEN serviceprovideridentificationcodequalifier = 'XX'
        THEN serviceprovideridentificationcode
    END                                         AS rndrg_prov_npi,
    renderingproviderreferenceidentification
                                                AS rndrg_prov_vdr_id,
    CASE WHEN serviceprovideridentificationcodequalifier = 'FI'
        THEN serviceprovideridentificationcode
    END                                         AS rndrg_prov_tax_id,
    CASE WHEN serviceprovideridentificationcodequalifier = 'FI'
            AND serviceproviderentitytypequalifier = '1'
        THEN serviceprovideridentificationcode
    END                                         AS rndrg_prov_ssn,
    CASE WHEN serviceprovideridentificationcodequalifier = 'SL'
        THEN serviceprovideridentificationcode
    END                                         AS rndrg_prov_state_lic_id,
    CASE WHEN serviceprovideridentificationcodequalifier = 'UP'
        THEN serviceprovideridentificationcode
    END                                         AS rndrg_prov_upin,
    CASE WHEN serviceprovideridentificationcodequalifier = 'PC'
        THEN serviceprovideridentificationcode
    END                                         AS rndrg_prov_comrcl_id,
    serviceproviderlastname                     AS rndrg_prov_1_nm,
    serviceproviderfirstname                    AS rndrg_prov_2_nm,
    CONCAT_WS(' ', crossovercarrierfirstname, crossovercarrierlastname)
                                                AS cob_pyr_nm,
    crossovercarrierentityidentifiercode        AS cob_pyr_id,
    crossovercarrierentitytypequalifier         AS cob_pyr_id_qual,
    coverdaysorvisitscount                      AS covrd_day_vst_cnt,
    ppsoperatingoutlieramount                   AS pps_operg_outlr_amt,
    lifetimepsychiatricdayscount                AS lftm_psychtrc_day_cnt,
    claimdrgamount                              AS clm_drg_amt,
    claimdisproportionateshareamount            AS clm_dsh_amt,
    claimmsppassthroughamount                   AS clm_msp_pass_thru_amt,
    claimppscapitalamount                       AS clm_pps_captl_amt,
    ppsoperatingfederalspecificdrgamount        AS pps_captl_fsp_drg_amt,
    ppscapitalhspdrgamount                      AS pps_captl_hsp_drg_amt,
    ppscapitaldshdrgamount                      AS pps_captl_dsh_drg_amt,
    oldcapitalamount                            AS prev_rptg_perd_captl_amt,
    ppscapitalimeamount                         AS pps_captl_ime_amt,
    ppsoperatinghospitalspecificdrgamount       AS pps_operg_hsp_drg_amt,
    costreportdaycount                          AS cost_rpt_day_cnt,
    ppscapitalfspdrgamount                      AS pps_operg_fsp_drg_amt,
    claimppscapitaloutlieramount                AS clm_pps_captl_outlr_amt,
    claimindirectteachingamount                 AS clm_indrct_tchg_amt,
    x.explode_idx + 1                           AS clm_pymt_remrk_cd_seq_num,
    UPPER(dense_clm_pymt_remrk_cd[x.explode_idx])
                                                AS clm_pymt_remrk_cd,
    ppscapitalexceptionamount                   AS pps_captl_excptn_amt,
    x2.explode_idx + 1                          AS clm_amt_seq_num,
    CASE WHEN dense_clm_amt[x2.explode_idx][1] != 'T'
        THEN dense_clm_amt[x2.explode_idx][0]
    END                                         AS clm_amt,
    dense_clm_amt[x2.explode_idx][1]            AS clm_amt_qual,
    x3.explode_idx + 1                          AS clm_qty_seq_num,
    dense_clm_qty[x3.explode_idx][0]            AS clm_qty,
    dense_clm_qty[x3.explode_idx][1]            AS clm_qty_qual,
    x4.explode_idx + 1                          AS clm_adjmt_seq_num,
    adjustmentgroupcode                         AS clm_adjmt_grp_cd,
    dense_clm_adjmt[x4.explode_idx][0]          AS clm_adjmt_rsn_txt,
    dense_clm_adjmt[x4.explode_idx][1]          AS clm_adjmt_amt,
    dense_clm_adjmt[x4.explode_idx][2]          AS clm_adjmt_qty,
    groupcontrolnumber                          AS clm_fnctnl_grp_ctl_num
FROM
    summary_tmp
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4)) as explode_idx) x
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) as explode_idx) x2
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) as explode_idx) x3
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5)) as explode_idx) x4
    WHERE 
        (dense_clm_pymt_remrk_cd[x.explode_idx] != NULL OR x.explode_idx = 0)
        AND
        (dense_clm_amt[x2.explode_idx] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x2.explode_idx = 0)
        AND
        (dense_clm_qty[x3.explode_idx] != CAST(ARRAY(NULL, NULL) AS ARRAY<STRING>) OR x3.explode_idx = 0)
        AND
        (dense_clm_adjmt[x4.explode_idx] != CAST(ARRAY(NULL, NULL, NULL) AS ARRAY<STRING>) OR x4.explode_idx = 0)
