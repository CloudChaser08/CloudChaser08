SELECT
    '04'                                    AS mdl_vrsn_num,
    ord.dataset                             AS data_set_nm,
    ord.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        ord.reportingenterpriseid,
        ord.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        ord.reportingenterpriseid,
        ord.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(ord.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(ord.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS prov_ord_dt,
    ord.orderinghcpprimarytaxonomy          AS ordg_prov_nucc_taxnmy_cd,
    ord.orderinghcpzipcode                  AS ordg_prov_zip_cd,
    ref1.gen_ref_cd                         AS prov_ord_ctgy_cd,
    CASE WHEN ref1.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END                                 AS prov_ord_ctgy_cd_qual,
    ref1.gen_ref_itm_nm                     AS prov_ord_ctgy_nm,
    ref2.gen_ref_cd                         AS prov_ord_typ_cd,
    CASE WHEN ref2.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END                                 AS prov_ord_typ_cd_qual,
    ref2.gen_ref_itm_nm                     AS prov_ord_typ_nm,
    CASE WHEN cpt_codes.code IS NOT NULL THEN cpt_codes.code
        WHEN hcpcs_codes.hcpc IS NOT NULL THEN hcpcs_codes.hcpc
        WHEN icd_diag_codes.code IS NOT NULL
            THEN clean_up_diagnosis_code(icd_diag_codes.code, NULL,
                COALESCE(ord.prov_ord_dt, ord.enc_dt))
        WHEN icd_proc_codes.code IS NOT NULL THEN icd_proc_codes.code
        WHEN loinc_codes.loinc_num IS NOT NULL THEN loinc_codes.loinc_num
        WHEN regexp_extract(ord.clean_actcode, '(^NG[0-9]+$)') != '' THEN ord.clean_actcode
        WHEN ref3.gen_ref_cd IS NOT NULL THEN ref3.gen_ref_cd
        ELSE NULL END                       AS prov_ord_cd,
    CASE WHEN cpt_codes.code IS NOT NULL OR
            hcpcs_codes.hcpc IS NOT NULL OR
            icd_diag_codes.code IS NOT NULL OR
            icd_proc_codes.code IS NOT NULL OR
            loinc_codes.loinc_num IS NOT NULL OR
            regexp_extract(ord.clean_actcode, '(^NG[0-9]+$)') != '' OR
            ref3.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        ELSE NULL END                       AS prov_ord_cd_qual,
    UPPER(COALESCE(ord.acttext, ord.acttextdisplay, ord.actdescription))
                                            AS prov_ord_alt_cd,
    'VENDOR'                                AS prov_ord_alt_cd_qual,
    UPPER(COALESCE(ord.acttextdisplay, ord.actdescription, ord.acttext))
                                            AS prov_ord_alt_nm,
    UPPER(COALESCE(ord.actdescription, ord.acttextdisplay, ord.acttext))
                                            AS prov_ord_alt_desc,
    CASE WHEN diag2.code IS NOT NULL
            THEN clean_up_diagnosis_code(diag2.code, NULL,
                COALESCE(ord.prov_ord_dt, ord.enc_dt))
        ELSE NULL END                       AS prov_ord_diag_cd,
    TRIM(UPPER(ord.actdiagnosis))           AS prov_ord_diag_nm,
    ord.vcxcode                             AS prov_ord_vcx_cd,
    CASE WHEN ord.vcxcode IS NOT NULL THEN 'CVX'
        ELSE NULL END                       AS prov_ord_vcx_cd_qual,
    UPPER(ord.actreasoncode)                AS prov_ord_rsn_cd,
    'VENDOR'                                AS prov_ord_rsn_cd_qual,
    clean_up_freetext(ord.orderedreason, false)
                                            AS prov_ord_rsn_nm,
    UPPER(ord.actstatus)                    AS prov_ord_stat_cd,
    'VENDOR'                                AS prov_ord_stat_cd_qual,
    CASE WHEN ord.completed = '0' THEN 'N'
        WHEN ord.completed = '1' THEN 'Y'
        ELSE NULL END                       AS prov_ord_complt_flg,
    extract_date(
        substring(ord.completedate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS prov_ord_complt_dt,
    clean_up_freetext(ord.completedreason, false)
                                            AS prov_ord_complt_rsn_cd,
    clean_up_freetext(ord.cancelledreason, false)
                                            AS prov_ord_cxld_rsn_cd,
    extract_date(
        substring(ord.CancelledDate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS prov_ord_cxld_dt,
    UPPER(ord.obsvalue)                     AS prov_ord_result_nm,
    clean_up_freetext(ord.obsinterpretation, false)
                                            AS prov_ord_result_desc,
    extract_date(
        substring(ord.ReceivedDate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS prov_ord_result_rcvd_dt,
    clean_up_freetext(ord.therapytype, false)
                                            AS prov_ord_trtmt_typ_cd,
    'VENDOR'                                AS prov_ord_trtmt_typ_cd_qual,
    UPPER(ord.refertospecialty)             AS prov_ord_rfrd_speclty_cd,
    'VENDOR'                                AS prov_ord_rfrd_speclty_cd_qual,
    clean_up_freetext(ord.specinsttext, false)
                                            AS prov_ord_specl_instrs_desc,
    CASE WHEN ord.education = '0' THEN 'N'
        WHEN ord.education = '1' THEN 'Y'
        ELSE NULL END                       AS prov_ord_edctn_flg,
    extract_date(
        substring(ord.educationdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS prov_ord_edctn_dt,
    'order'                                 AS prmy_src_tbl_nm,
    extract_date(
        substring(ord.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM `ord_clean_actcodes` ord
    LEFT JOIN demographics_local dem ON ord.reportingenterpriseid = dem.reportingenterpriseid
        AND ord.nextgengroupid = dem.nextgengroupid
        AND COALESCE(
                substring(ord.encounterdate, 1, 8),
                substring(ord.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(ord.encounterdate, 1, 8),
                substring(ord.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'order.actmood'
        AND ord.actmood = ref1.gen_ref_cd
        AND ref1.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref2 ON ref2.hvm_vdr_feed_id = 35
        AND ref2.gen_ref_domn_nm = 'order.actclass'
        AND ord.actclass = ref2.gen_ref_cd
        AND ref2.whtlst_flg = 'Y'
    -- NG is a very common value in this field and slows down transformation significantly
    -- (It's 20x more common then the next most common value)
    LEFT JOIN cpt_codes ON ord.clean_actcode = cpt_codes.code
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN hcpcs_codes ON ord.clean_actcode = hcpc
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN icd_diag_codes ON ord.clean_actcode = icd_diag_codes.code
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN icd_proc_codes ON ord.clean_actcode = icd_proc_codes.code
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN loinc_codes ON translate(ord.clean_actcode, '-', '') = loinc_codes.loinc_num
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN (SELECT DISTINCT gen_ref_cd
            FROM ref_gen_ref
            WHERE gen_ref_domn_nm = 'emr_prov_ord.prov_ord_cd'
                AND whtlst_flg = 'Y'
        ) ref3
        ON ord.clean_actcode = ref3.gen_ref_cd
            AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
    LEFT JOIN icd_diag_codes diag2 ON clean_up_freetext(ord.actdiagnosiscode, true) = diag2.code
        AND ord.clean_actdiagnosiscode IS NOT NULL AND ord.clean_actdiagnosiscode != ''
