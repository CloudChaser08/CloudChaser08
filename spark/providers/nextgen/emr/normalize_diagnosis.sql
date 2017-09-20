INSERT INTO diagnosis_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_diag_id
    NULL,                                   -- crt_dt
    '05',                                   -- mdl_vrsn_num
    diag.dataset,                           -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    diag.reportingenterpriseid,             -- vdr_org_id
    NULL,                                   -- vdr_diag_id
    NULL,                                   -- vdr_diag_id_qual
    concat_ws('_', 'NG',
        diag.reportingenterpriseid,
        diag.nextgengroupid),               -- hvid
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
        diag.reportingenterpriseid,
        diag.encounter_id),                 -- hv_enc_id
    extract_date(
        substring(diag.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(diag.diagnosisdate, 1, 8), '%Y%m%d', CAST({diag_min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- diag_dt
    NULL,                                   -- diag_rndrg_fclty_npi
    NULL,                                   -- diag_rndrg_fclty_vdr_id
    NULL,                                   -- diag_rndrg_fclty_vdr_id_qual
    NULL,                                   -- diag_rndrg_fclty_alt_id
    NULL,                                   -- diag_rndrg_fclty_alt_id_qual
    NULL,                                   -- diag_rndrg_fclty_tax_id
    NULL,                                   -- diag_rndrg_fclty_dea_id
    NULL,                                   -- diag_rndrg_fclty_state_lic_id
    NULL,                                   -- diag_rndrg_fclty_comrcl_id
    NULL,                                   -- diag_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- diag_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- diag_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- diag_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- diag_rndrg_fclty_alt_speclty_id
    NULL,                                   -- diag_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- diag_rndrg_fclty_nm
    NULL,                                   -- diag_rndrg_fclty_addr_1_txt
    NULL,                                   -- diag_rndrg_fclty_addr_2_txt
    NULL,                                   -- diag_rndrg_fclty_state_cd
    NULL,                                   -- diag_rndrg_fclty_zip_cd
    NULL,                                   -- diag_rndrg_prov_npi
    NULL,                                   -- diag_rndrg_prov_vdr_id
    NULL,                                   -- diag_rndrg_prov_vdr_id_qual
    NULL,                                   -- diag_rndrg_prov_alt_id
    NULL,                                   -- diag_rndrg_prov_alt_id_qual
    NULL,                                   -- diag_rndrg_prov_tax_id
    NULL,                                   -- diag_rndrg_prov_dea_id
    NULL,                                   -- diag_rndrg_prov_state_lic_id
    NULL,                                   -- diag_rndrg_prov_comrcl_id
    NULL,                                   -- diag_rndrg_prov_upin
    NULL,                                   -- diag_rndrg_prov_ssn
    NULL,                                   -- diag_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- diag_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- diag_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- diag_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- diag_rndrg_prov_alt_speclty_id
    NULL,                                   -- diag_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- diag_rndrg_prov_frst_nm
    NULL,                                   -- diag_rndrg_prov_last_nm
    NULL,                                   -- diag_rndrg_prov_addr_1_txt
    NULL,                                   -- diag_rndrg_prov_addr_2_txt
    NULL,                                   -- diag_rndrg_prov_state_cd
    NULL,                                   -- diag_rndrg_prov_zip_cd
    extract_date(
        substring(diag.onsetdate, 1, 8), '%Y%m%d', CAST({diag_min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- diag_onset_dt
    extract_date(
        substring(diag.dateresolved, 1, 8), '%Y%m%d', CAST({diag_min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- diag_resltn_dt
    diag.emrcode,                           -- diag_cd
    NULL,                                   -- diag_cd_qual
    NULL,                                   -- diag_alt_cd
    NULL,                                   -- diag_alt_cd_qual
    NULL,                                   -- diag_nm
    NULL,                                   -- diag_desc
    NULL,                                   -- diag_prty_cd
    NULL,                                   -- diag_prty_cd_qual
    diag.dxpriority,                        -- diag_svty_cd
    NULL,                                   -- diag_svty_cd_qual
    NULL,                                   -- diag_resltn_cd
    NULL,                                   -- diag_resltn_cd_qual
    NULL,                                   -- diag_resltn_nm
    NULL,                                   -- diag_resltn_desc
    clean_up_numeric_code(diag.statusid),   -- diag_stat_cd
    NULL,                                   -- diag_stat_cd_qual
    ref1.gen_ref_itm_nm,                    -- diag_stat_nm
    clean_up_freetext(diag.statusidtext, false),
                                            -- diag_stat_desc
    NULL,                                   -- diag_snomed_cd
    NULL,                                   -- diag_meth_cd
    NULL,                                   -- diag_meth_cd_qual
    NULL,                                   -- diag_meth_nm
    NULL,                                   -- diag_meth_desc
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'diagnosis'                             -- prmy_src_tbl_nm
FROM diagnosis diag
    LEFT JOIN demographics_dedup dem ON diag.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND diag.NextGenGroupID = dem.NextGenGroupID
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'diagnosis.statusid'
        AND diag.statusid = ref1.gen_ref_cd
        AND ref1.whtlst_flg = 'Y';
