SELECT
    CONCAT('42_', service.id)                   AS hv_medcl_clm_pymt_dtl_id,
    '005010X221A1'                              AS src_vrsn_id,
    CONCAT('42_', claim.id)                     AS vdr_medcl_clm_pymt_sumry_id,
    'VENDOR'                                    AS vdr_medcl_clm_pymt_sumry_id_qual,
    service.id                                  AS vdr_medcl_clm_pymt_dtl_id,
    'VENDOR'                                    AS vdr_medcl_clm_pymt_dtl_id_qual,
    claim.master_patient_id                     AS hvid,
    extract_date(
        COALESCE(service.servicedate, claim.claimstartperiod),
        '%Y%m%d'
    )                                           AS svc_ln_start_dt,
    extract_date(
        COALESCE(service.servicedate, claim.claimendperiod),
        '%Y%m%d'
    )                                           AS svc_ln_end_dt,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone)  = 'HPI' THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo)   = 'HPI' THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) = 'HPI' THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour)  = 'HPI' THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive)  = 'HPI' THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix)   = 'HPI' THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) = 'HPI' THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) = 'HPI' THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine)  = 'HPI' THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten)   = 'HPI' THEN renderingproviderten
        END
    END                                         AS rndrg_prov_npi,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone) IS NULL AND renderingproviderone IS NOT NULL
                THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo) IS NULL AND renderingprovidertwo IS NOT NULL
                THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) IS NULL AND renderingproviderthree IS NOT NULL
                THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour) IS NULL AND renderingproviderfour IS NOT NULL
                THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive) IS NULL AND renderingproviderfive IS NOT NULL
                THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix) IS NULL AND renderingprovidersix IS NOT NULL
                THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) IS NULL AND renderingproviderseven IS NOT NULL
                THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) IS NULL AND renderingprovidereight IS NOT NULL
                THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine) IS NULL AND renderingprovidernine IS NOT NULL
                THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten) IS NULL AND renderingproviderten IS NOT NULL
                THEN renderingproviderten
        END
    END                                         AS rndrg_prov_vdr_id,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone)  = 'TJ' THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo)   = 'TJ' THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) = 'TJ' THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour)  = 'TJ' THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive)  = 'TJ' THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix)   = 'TJ' THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) = 'TJ' THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) = 'TJ' THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine)  = 'TJ' THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten)   = 'TJ' THEN renderingproviderten
        END
    END                                         AS rndrg_prov_tax_id,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone)  = 'SY' THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo)   = 'SY' THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) = 'SY' THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour)  = 'SY' THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive)  = 'SY' THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix)   = 'SY' THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) = 'SY' THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) = 'SY' THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine)  = 'SY' THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten)   = 'SY' THEN renderingproviderten
        END
    END                                         AS rndrg_prov_ssn,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone)  = '0B' THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo)   = '0B' THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) = '0B' THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour)  = '0B' THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive)  = '0B' THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix)   = '0B' THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) = '0B' THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) = '0B' THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine)  = '0B' THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten)   = '0B' THEN renderingproviderten
        END
    END                                         AS rndrg_prov_state_lic_id,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone)  = '1G' THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo)   = '1G' THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) = '1G' THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour)  = '1G' THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive)  = '1G' THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix)   = '1G' THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) = '1G' THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) = '1G' THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine)  = '1G' THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten)   = '1G' THEN renderingproviderten
        END
    END                                         AS rndrg_prov_upin,
    CASE WHEN REGEXP_EXTRACT(CONCAT('00', claim.facilitytypecode), '(..)$') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            OR SUBSTR(claim.frequencycode, 1, 1) = '3'
                THEN NULL
        ELSE CASE WHEN UPPER(renderingprovideridentificationqualifierone)  = 'G2' THEN renderingproviderone
            WHEN UPPER(renderingprovideridentificationqualifiertwo)   = 'G2' THEN renderingprovidertwo
            WHEN UPPER(renderingprovideridentificationqualifierthree) = 'G2' THEN renderingproviderthree
            WHEN UPPER(renderingprovideridentificationqualifierfour)  = 'G2' THEN renderingproviderfour
            WHEN UPPER(renderingprovideridentificationqualifierfive)  = 'G2' THEN renderingproviderfive
            WHEN UPPER(renderingprovideridentificationqualifiersix)   = 'G2' THEN renderingprovidersix
            WHEN UPPER(renderingprovideridentificationqualifierseven) = 'G2' THEN renderingproviderseven
            WHEN UPPER(renderingprovideridentificationqualifiereight) = 'G2' THEN renderingprovidereight
            WHEN UPPER(renderingprovideridentificationqualifiernine)  = 'G2' THEN renderingprovidernine
            WHEN UPPER(renderingprovideridentificationqualifierten)   = 'G2' THEN renderingproviderten
        END
    END                                         AS rndrg_prov_comrcl_id,
    CASE WHEN service.productserviceidqualifier NOT IN ('NU', 'N4')
        THEN clean_up_procedure_code(service.productserviceid)
    END                                         AS adjctd_proc_cd,
    CASE WHEN service.productserviceidqualifier NOT IN ('NU', 'N4')
            AND clean_up_procedure_code(service.productserviceid) IS NOT NULL
        THEN service.productserviceidqualifier  
    END                                         AS adjctd_proc_cd_qual,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.proceduremodifierone), 1, 2))                
                                                AS adjctd_proc_cd_1_modfr,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.proceduremodifiertwo), 1, 2))                
                                                AS adjctd_proc_cd_2_modfr,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.proceduremodifierthree), 1, 2))                
                                                AS adjctd_proc_cd_3_modfr,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.proceduremodifierfour), 1, 2))
                                                AS adjctd_proc_cd_4_modfr,
    clean_up_procedure_code(service.secondaryproductserviceid)
                                                AS orig_submtd_adjctd_proc_cd,
    CASE WHEN clean_up_procedure_code(service.secondaryproductserviceid) IS NOT NULL
        THEN service.secondaryproductserviceidqualifier  
    END                                         AS orig_submtd_adjctd_proc_cd_qual,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.secondaryproceduremodifierone), 1, 2))
                                                AS orig_submtd_adjctd_proc_cd_1_modfr,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.secondaryproceduremodifiertwo), 1, 2))
                                                AS orig_submtd_adjctd_proc_cd_2_modfr,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.secondaryproceduremodifierthree), 1, 2))
                                                AS orig_submtd_adjctd_proc_cd_3_modfr,
    UPPER(SUBSTR(clean_up_alphanumeric_code(service.secondaryproceduremodifierfour), 1, 2))
                                                AS orig_submtd_adjctd_proc_cd_4_modfr,
    service.lineitemchargeamount                AS svc_ln_submtd_chg_amt,
    service.lineitemproviderpayment             AS svc_ln_prov_pymt_amt,
    densify_scalar_array(ARRAY(
        service.nubcrevenuecode,
        CASE WHEN service.productserviceidqualifier = 'NU' THEN service.productserviceid END
    ))[x.explode_idx]                           AS rev_cd,
    CASE WHEN service.productserviceidqualifier = 'N4'
        THEN service.productserviceid END       AS ndc_cd,
    service.quantity                            AS paid_svc_unt_cnt,
    service.secondaryquantity                   AS orig_svc_unt_cnt,
    adjust.adjustmentgroupcode                  AS svc_ln_adjmt_grp_cd,
    x2.explode_idx + 1                          AS svc_ln_adjmt_seq_num,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x2.explode_idx][0]                       AS svc_ln_adjmt_rsn_txt,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x2.explode_idx][1]                       AS svc_ln_adjmt_amt,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x2.explode_idx][2]                       AS svc_ln_adjmt_qty,
    MD5(service.lineitemcontrolnumber)          AS svc_ln_prov_ctl_num
FROM
    serviceline service
    LEFT JOIN
        serviceline_adjustment adjust
            ON service.id = adjust.remitserviceline_id
    LEFT JOIN
        remit_claim claim
            ON service.remitclaim_id = claim.id
    CROSS JOIN (SELECT explode(array(0, 1)) as explode_idx) x
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5)) as explode_idx) x2
WHERE
    (densify_scalar_array(ARRAY(
        service.nubcrevenuecode,
        CASE WHEN service.productserviceidqualifier = 'NU' THEN service.productserviceid END
    ))[x.explode_idx] IS NOT NULL OR x.explode_idx = 0)
    AND
    (densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))[x2.explode_idx] != CAST(ARRAY(NULL, NULL, NULL) AS ARRAY<STRING>) OR x2.explode_idx = 0)
