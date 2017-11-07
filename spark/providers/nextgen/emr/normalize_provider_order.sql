DROP TABLE IF EXISTS ord_clean_actcodes;
CREATE TABLE ord_clean_actcodes AS
SELECT
    *,
    UPPER(ord.actcode) as clean_actcode,
    clean_up_freetext(ord.actdiagnosiscode, false) as clean_actdiagnosiscode,
    extract_date(
        substring(ord.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ) as enc_dt,
    extract_date(
        substring(ord.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ) as prov_ord_dt
FROM `order` ord;

INSERT INTO provider_order_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_prov_ord_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    ord.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    ord.reportingenterpriseid,              -- vdr_org_id
    NULL,                                   -- vdr_prov_ord_id
    NULL,                                   -- vdr_prov_ord_id_qual
    concat_ws('_', 'NG',
        ord.reportingenterpriseid,
        ord.nextgengroupid),                -- hvid
    dem.birthyear,                          -- ptnt_birth_yr
    NULL,                                   -- ptnt_age_num
    NULL,                                   -- ptnt_lvg_flg
    NULL,                                   -- ptnt_dth_dt
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END,                       -- ptnt_gender_cd
    NULL,                                   -- ptnt_state_cd
    dem.zip3,                               -- ptnt_zip3_cd
    concat_ws('_', '35',
        ord.reportingenterpriseid,
        ord.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(ord.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(ord.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- prov_ord_dt
    NULL,                                   -- ordg_prov_npi
    NULL,                                   -- ordg_prov_vdr_id
    NULL,                                   -- ordg_prov_vdr_id_qual
    NULL,                                   -- ordg_prov_alt_id
    NULL,                                   -- ordg_prov_alt_id_qual
    NULL,                                   -- ordg_prov_tax_id
    NULL,                                   -- ordg_prov_dea_id
    NULL,                                   -- ordg_prov_state_lic_id
    NULL,                                   -- ordg_prov_comrcl_id
    NULL,                                   -- ordg_prov_upin
    NULL,                                   -- ordg_prov_ssn
    ord.orderinghcpprimarytaxonomy,         -- ordg_prov_nucc_taxnmy_cd
    NULL,                                   -- ordg_prov_alt_taxnmy_id
    NULL,                                   -- ordg_prov_alt_taxnmy_id_qual
    NULL,                                   -- ordg_prov_mdcr_speclty_cd
    NULL,                                   -- ordg_prov_alt_speclty_id
    NULL,                                   -- ordg_prov_alt_speclty_id_qual
    NULL,                                   -- ordg_prov_fclty_nm
    NULL,                                   -- ordg_prov_frst_nm
    NULL,                                   -- ordg_prov_last_nm
    NULL,                                   -- ordg_prov_addr_1_txt
    NULL,                                   -- ordg_prov_addr_2_txt
    NULL,                                   -- ordg_prov_state_cd
    ord.orderinghcpzipcode,                 -- ordg_prov_zip_cd
    NULL,                                   -- prov_ord_start_dt
    NULL,                                   -- prov_ord_end_dt
    ref1.gen_ref_cd,                        -- prov_ord_ctgy_cd
    CASE WHEN ref1.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END,                                -- prov_ord_ctgy_cd_qual
    ref1.gen_ref_itm_nm,                    -- prov_ord_ctgy_nm
    NULL,                                   -- prov_ord_ctgy_desc
    ref2.gen_ref_cd,                        -- prov_ord_typ_cd
    CASE WHEN ref2.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END,                                -- prov_ord_typ_cd_qual
    ref2.gen_ref_itm_nm,                    -- prov_ord_typ_nm
    NULL,                                   -- prov_ord_typ_desc
    CASE WHEN cpt_codes.code IS NOT NULL THEN cpt_codes.code
        WHEN hcpcs_codes.hcpc IS NOT NULL THEN hcpcs_codes.hcpc
        WHEN icd_diag_codes.code IS NOT NULL
            THEN clean_up_diagnosis_code(icd_diag_codes.code, NULL,
                COALESCE(ord.prov_ord_dt, ord.enc_dt))
        WHEN icd_proc_codes.code IS NOT NULL THEN icd_proc_codes.code
        WHEN loinc_codes.loinc_num IS NOT NULL THEN loinc_codes.loinc_num
        WHEN regexp_extract(ord.clean_actcode, '(^NG[0-9]+$)') != '' THEN ord.clean_actcode
        WHEN ref3.gen_ref_cd IS NOT NULL THEN ref3.gen_ref_cd
        ELSE NULL END,                      -- prov_ord_cd
    CASE WHEN cpt_codes.code IS NOT NULL OR
            hcpcs_codes.hcpc IS NOT NULL OR
            icd_diag_codes.code IS NOT NULL OR
            icd_proc_codes.code IS NOT NULL OR
            loinc_codes.loinc_num IS NOT NULL OR
            regexp_extract(ord.clean_actcode, '(^NG[0-9]+$)') != '' OR
            ref3.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        ELSE NULL END,                      -- prov_ord_cd_qual
    NULL,                                   -- prov_ord_nm
    NULL,                                   -- prov_ord_desc
    UPPER(COALESCE(ord.acttext, ord.acttextdisplay, ord.actdescription)),
                                            -- prov_ord_alt_cd
    'VENDOR',                               -- prov_ord_alt_cd_qual
    UPPER(COALESCE(ord.acttext, ord.acttextdisplay, ord.actdescription)),
                                            -- prov_ord_alt_nm
    UPPER(COALESCE(ord.acttext, ord.acttextdisplay, ord.actdescription)),
                                            -- prov_ord_alt_desc
    CASE WHEN diag2.code IS NOT NULL
            THEN clean_up_diagnosis_code(diag2.code, NULL,
                COALESCE(ord.prov_ord_dt, ord.enc_dt))
        ELSE NULL END,                      -- prov_ord_diag_cd
    NULL,                                   -- prov_ord_diag_cd_qual
    clean_up_freetext(ord.actdiagnosis, false),
                                            -- prov_ord_diag_nm
    NULL,                                   -- prov_ord_diag_desc
    NULL,                                   -- prov_ord_snomed_cd
    ord.vcxcode,                            -- prov_ord_vcx_cd
    CASE WHEN ord.vcxcode IS NOT NULL THEN 'CVX'
        ELSE NULL END,                      -- prov_ord_vcx_cd_qual
    NULL,                                   -- prov_ord_vcx_nm
    NULL,                                   -- prov_ord_vcx_desc
    UPPER(ord.actreasoncode),               -- prov_ord_rsn_cd
    'VENDOR',                               -- prov_ord_rsn_cd_qual
    clean_up_freetext(ord.orderedreason, false),
                                            -- prov_ord_rsn_nm
    NULL,                                   -- prov_ord_rsn_desc
    UPPER(ord.actstatus),                   -- prov_ord_stat_cd
    'VENDOR',                               -- prov_ord_stat_cd_qual
    NULL,                                   -- prov_ord_stat_nm
    NULL,                                   -- prov_ord_stat_desc
    CASE WHEN ord.completed = '0' THEN 'N'
        WHEN ord.completed = '1' THEN 'Y'
        ELSE NULL END,                      -- prov_ord_complt_flg
    extract_date(
        substring(ord.completedate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- prov_ord_complt_dt
    clean_up_freetext(ord.completedreason, false),
                                            -- prov_ord_complt_rsn_cd
    NULL,                                   -- prov_ord_complt_rsn_cd_qual
    NULL,                                   -- prov_ord_complt_rsn_nm
    NULL,                                   -- prov_ord_complt_rsn_desc
    clean_up_freetext(ord.cancelledreason, false),
                                            -- prov_ord_cxld_rsn_cd
    NULL,                                   -- prov_ord_cxld_rsn_cd_qual
    NULL,                                   -- prov_ord_cxld_rsn_nm
    NULL,                                   -- prov_ord_cxld_rsn_desc
    extract_date(
        substring(ord.CancelledDate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- prov_ord_cxld_dt
    NULL,                                   -- prov_ord_result_cd
    NULL,                                   -- prov_ord_result_cd_qual
    ord.obsvalue,                           -- prov_ord_result_nm
    clean_up_freetext(ord.obsinterpretation, false),
                                            -- prov_ord_result_desc
    extract_date(
        substring(ord.ReceivedDate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- prov_ord_result_rcvd_dt
    clean_up_freetext(ord.therapytype, false),
                                            -- prov_ord_trtmt_typ_cd
    'VENDOR',                               -- prov_ord_trtmt_typ_cd_qual
    NULL,                                   -- prov_ord_trtmt_typ_nm
    NULL,                                   -- prov_ord_trtmt_typ_desc
    UPPER(ord.refertospecialty),            -- prov_ord_rfrd_speclty_cd
    'VENDOR',                               -- prov_ord_rfrd_speclty_cd_qual
    NULL,                                   -- prov_ord_rfrd_speclty_nm
    NULL,                                   -- prov_ord_rfrd_speclty_desc
    NULL,                                   -- prov_ord_specl_instrs_cd
    NULL,                                   -- prov_ord_specl_instrs_cd_qual
    NULL,                                   -- prov_ord_specl_instrs_nm
    clean_up_freetext(ord.specinsttext, false),
                                            -- prov_ord_specl_instrs_desc
    CASE WHEN ord.education = '0' THEN 'N'
        WHEN ord.education = '1' THEN 'Y'
        ELSE NULL END,                      -- prov_ord_edctn_flg
    extract_date(
        substring(ord.educationdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- prov_ord_edctn_dt
    NULL,                                   -- prov_ord_edctn_cd
    NULL,                                   -- prov_ord_edctn_cd_qual
    NULL,                                   -- prov_ord_edctn_nm
    NULL,                                   -- prov_ord_edctn_desc
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'order',                                -- prmy_src_tbl_nm
    extract_date(
        substring(ord.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
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
    LEFT JOIN ref_gen_ref ref3 ON ord.clean_actcode = ref3.gen_ref_cd
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ref3.gen_ref_domn_nm = 'emr_prov_ord.prov_ord_cd'
        AND ref3.whtlst_flg = 'Y'
    LEFT JOIN icd_diag_codes diag2 ON clean_up_freetext(ord.actdiagnosiscode, true) = diag2.code
        AND ord.clean_actdiagnosiscode IS NOT NULL AND ord.clean_actdiagnosiscode != '';
