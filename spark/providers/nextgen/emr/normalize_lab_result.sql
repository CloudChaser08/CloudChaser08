INSERT INTO lab_result_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_lab_result_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    rslt.dataset,                           -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    rslt.reportingenterpriseid,             -- vdr_org_id
    NULL,                                   -- vdr_lab_test_id
    NULL,                                   -- vdr_lab_test_id_qual
    NULL,                                   -- vdr_lab_result_id
    NULL,                                   -- vdr_lab_result_id_qual
    concat_ws('_', 'NG',
        rslt.reportingenterpriseid,
        rslt.nextgengroupid),               -- hvid
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
        rslt.reportingenterpriseid,
        rslt.encounter_id),                 -- hv_enc_id
    extract_date(
        substring(rslt.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    CASE WHEN rslt.ordernum IS NOT NULL THEN concat_ws('_', '35',
            rslt.reportingenterpriseid,
            rslt.ordernum)
        ELSE NULL END,                      -- hv_lab_ord_id
    extract_date(
        substring(rslt.collectiontime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- lab_test_smpl_collctn_dt
    NULL,                                   -- lab_test_schedd_dt
    NULL,                                   -- lab_test_execd_dt
    NULL,                                   -- lab_result_dt
    NULL,                                   -- lab_test_ordg_prov_npi
    NULL,                                   -- lab_test_ordg_prov_vdr_id
    NULL,                                   -- lab_test_ordg_prov_vdr_id_qual
    NULL,                                   -- lab_test_ordg_prov_alt_id
    NULL,                                   -- lab_test_ordg_prov_alt_id_qual
    NULL,                                   -- lab_test_ordg_prov_tax_id
    NULL,                                   -- lab_test_ordg_prov_dea_id
    NULL,                                   -- lab_test_ordg_prov_state_lic_id
    NULL,                                   -- lab_test_ordg_prov_comrcl_id
    NULL,                                   -- lab_test_ordg_prov_upin
    NULL,                                   -- lab_test_ordg_prov_ssn
    NULL,                                   -- lab_test_ordg_prov_nucc_taxnmy_cd
    NULL,                                   -- lab_test_ordg_prov_alt_taxnmy_id
    NULL,                                   -- lab_test_ordg_prov_alt_taxnmy_id_qual
    NULL,                                   -- lab_test_ordg_prov_mdcr_speclty_cd
    NULL,                                   -- lab_test_ordg_prov_alt_speclty_id
    NULL,                                   -- lab_test_ordg_prov_alt_speclty_id_qual
    NULL,                                   -- lab_test_ordg_prov_fclty_nm
    NULL,                                   -- lab_test_ordg_prov_frst_nm
    NULL,                                   -- lab_test_ordg_prov_last_nm
    NULL,                                   -- lab_test_ordg_prov_addr_1_txt
    NULL,                                   -- lab_test_ordg_prov_addr_2_txt
    NULL,                                   -- lab_test_ordg_prov_state_cd
    NULL,                                   -- lab_test_ordg_prov_zip_cd
    NULL,                                   -- lab_test_exectg_fclty_npi
    NULL,                                   -- lab_test_exectg_fclty_vdr_id
    NULL,                                   -- lab_test_exectg_fclty_vdr_id_qual
    NULL,                                   -- lab_test_exectg_fclty_alt_id
    NULL,                                   -- lab_test_exectg_fclty_alt_id_qual
    NULL,                                   -- lab_test_exectg_fclty_tax_id
    NULL,                                   -- lab_test_exectg_fclty_dea_id
    NULL,                                   -- lab_test_exectg_fclty_state_lic_id
    NULL,                                   -- lab_test_exectg_fclty_comrcl_id
    NULL,                                   -- lab_test_exectg_fclty_nucc_taxnmy_cd
    NULL,                                   -- lab_test_exectg_fclty_alt_taxnmy_id
    NULL,                                   -- lab_test_exectg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- lab_test_exectg_fclty_mdcr_speclty_cd
    NULL,                                   -- lab_test_exectg_fclty_alt_speclty_id
    NULL,                                   -- lab_test_exectg_fclty_alt_speclty_id_qual
    NULL,                                   -- lab_test_exectg_fclty_nm
    NULL,                                   -- lab_test_exectg_fclty_addr_1_txt
    NULL,                                   -- lab_test_exectg_fclty_addr_2_txt
    NULL,                                   -- lab_test_exectg_fclty_state_cd
    NULL,                                   -- lab_test_exectg_fclty_zip_cd
    NULL,                                   -- lab_test_specmn_typ_cd
    NULL,                                   -- lab_test_fstg_stat_flg
    NULL,                                   -- lab_test_panel_nm
    CASE WHEN translate(rslt.emrcode, '-', '') = clean_up_numeric_code(rslt.emrcode)
            THEN translate(rslt.emrcode, '-', '')
        ELSE ref.gen_ref_itm_nm END,        -- lab_test_nm
    NULL,                                   -- lab_test_desc
    translate(rslt.loinccode, '-', ''),     -- lab_test_loinc_cd
    clean_up_freetext(rslt.snomedcode, false),
                                            -- lab_test_snomed_cd
    clean_up_freetext(rslt.testcodeid, false),
                                            -- lab_test_vdr_cd
    NULL,                                   -- lab_test_vdr_cd_qual
    NULL,                                   -- lab_test_alt_cd
    NULL,                                   -- lab_test_alt_cd_qual
    CASE WHEN CAST(rslt.result as float) IS NOT NULL
            THEN rslt.result
        ELSE ref2.gen_ref_itm_nm END,       -- lab_result_nm
    NULL,                                   -- lab_result_desc
    NULL,                                   -- lab_result_msrmt
    NULL,                                   -- lab_result_uom
    NULL,                                   -- lab_result_qual
    NULL,                                   -- lab_result_abnorm_flg
    NULL,                                   -- lab_result_norm_min_msrmt
    NULL,                                   -- lab_result_norm_max_msrmt
    NULL,                                   -- lab_test_diag_cd
    NULL,                                   -- lab_test_diag_cd_qual
    extract_date(
        substring(rslt.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- data_captr_dt
    clean_up_freetext(rslt.ngnstatus, false),
                                            -- rec_stat_cd
    'labresult',                            -- prmy_src_tbl_nm
    extract_date(
        substring(rslt.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM labresult rslt
    LEFT JOIN demographics_local dem ON rslt.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND rslt.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(rslt.encounterdate, 1, 8),
                substring(rslt.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(rslt.encounterdate, 1, 8),
                substring(rslt.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref ON ref.gen_ref_domn_nm = 'emr_lab_result.lab_test_nm'
        AND TRIM(UPPER(rslt.emrcode)) = ref.gen_ref_itm_nm
        AND ref.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref2 ON ref2.gen_ref_domn_nm = 'emr_lab_result.lab_result_nm'
        AND TRIM(UPPER(rslt.result)) = ref2.gen_ref_itm_nm
        AND ref2.whtlst_flg = 'Y';


INSERT INTO lab_result_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_lab_result_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    lip.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    lip.reportingenterpriseid,              -- vdr_org_id
    NULL,                                   -- vdr_lab_test_id
    NULL,                                   -- vdr_lab_test_id_qual
    NULL,                                   -- vdr_lab_result_id
    NULL,                                   -- vdr_lab_result_id_qual
    concat_ws('_', 'NG',
        lip.reportingenterpriseid,
        lip.nextgengroupid) as hvid,        -- hvid
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
        lip.reportingenterpriseid,
        lip.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(lip.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    NULL,                                   -- hv_lab_ord_id
    NULL,                                   -- lab_test_smpl_collctn_dt
    NULL,                                   -- lab_test_schedd_dt
    extract_date(
        substring(lip.datadatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- lab_test_execd_dt
    extract_date(
        substring(lip.datadatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- lab_result_dt
    NULL,                                   -- lab_test_ordg_prov_npi
    NULL,                                   -- lab_test_ordg_prov_vdr_id
    NULL,                                   -- lab_test_ordg_prov_vdr_id_qual
    NULL,                                   -- lab_test_ordg_prov_alt_id
    NULL,                                   -- lab_test_ordg_prov_alt_id_qual
    NULL,                                   -- lab_test_ordg_prov_tax_id
    NULL,                                   -- lab_test_ordg_prov_dea_id
    NULL,                                   -- lab_test_ordg_prov_state_lic_id
    NULL,                                   -- lab_test_ordg_prov_comrcl_id
    NULL,                                   -- lab_test_ordg_prov_upin
    NULL,                                   -- lab_test_ordg_prov_ssn
    NULL,                                   -- lab_test_ordg_prov_nucc_taxnmy_cd
    NULL,                                   -- lab_test_ordg_prov_alt_taxnmy_id
    NULL,                                   -- lab_test_ordg_prov_alt_taxnmy_id_qual
    NULL,                                   -- lab_test_ordg_prov_mdcr_speclty_cd
    NULL,                                   -- lab_test_ordg_prov_alt_speclty_id
    NULL,                                   -- lab_test_ordg_prov_alt_speclty_id_qual
    NULL,                                   -- lab_test_ordg_prov_fclty_nm
    NULL,                                   -- lab_test_ordg_prov_frst_nm
    NULL,                                   -- lab_test_ordg_prov_last_nm
    NULL,                                   -- lab_test_ordg_prov_addr_1_txt
    NULL,                                   -- lab_test_ordg_prov_addr_2_txt
    NULL,                                   -- lab_test_ordg_prov_state_cd
    NULL,                                   -- lab_test_ordg_prov_zip_cd
    NULL,                                   -- lab_test_exectg_fclty_npi
    NULL,                                   -- lab_test_exectg_fclty_vdr_id
    NULL,                                   -- lab_test_exectg_fclty_vdr_id_qual
    NULL,                                   -- lab_test_exectg_fclty_alt_id
    NULL,                                   -- lab_test_exectg_fclty_alt_id_qual
    NULL,                                   -- lab_test_exectg_fclty_tax_id
    NULL,                                   -- lab_test_exectg_fclty_dea_id
    NULL,                                   -- lab_test_exectg_fclty_state_lic_id
    NULL,                                   -- lab_test_exectg_fclty_comrcl_id
    NULL,                                   -- lab_test_exectg_fclty_nucc_taxnmy_cd
    NULL,                                   -- lab_test_exectg_fclty_alt_taxnmy_id
    NULL,                                   -- lab_test_exectg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- lab_test_exectg_fclty_mdcr_speclty_cd
    NULL,                                   -- lab_test_exectg_fclty_alt_speclty_id
    NULL,                                   -- lab_test_exectg_fclty_alt_speclty_id_qual
    NULL,                                   -- lab_test_exectg_fclty_nm
    NULL,                                   -- lab_test_exectg_fclty_addr_1_txt
    NULL,                                   -- lab_test_exectg_fclty_addr_2_txt
    NULL,                                   -- lab_test_exectg_fclty_state_cd
    NULL,                                   -- lab_test_exectg_fclty_zip_cd
    NULL,                                   -- lab_test_specmn_typ_cd
    NULL,                                   -- lab_test_fstg_stat_flg
    'LIPID_PANEL',                          -- lab_test_panel_nm
    NULL,                                   -- lab_test_nm
    NULL,                                   -- lab_test_desc
    ARRAY('134577', '20859', '25718', '20933')[explode_idx],
                                            -- lab_test_loinc_cd
    NULL,                                   -- lab_test_snomed_cd
    NULL,                                   -- lab_test_vdr_cd
    NULL,                                   -- lab_test_vdr_cd_qual
    NULL,                                   -- lab_test_alt_cd
    NULL,                                   -- lab_test_alt_cd_qual
    NULL,                                   -- lab_result_nm
    NULL,                                   -- lab_result_desc
    ARRAY(lip.ldl, lip.hdl, lip.triglycerides, lip.totalcholesterol)[explode_idx],
                                            -- lab_result_msrmt
    ARRAY('mg/dl', 'mg/dl or mg/mL', 'mg/dl', 'mg/dl')[explode_idx],
                                            -- lab_result_uom
    ARRAY('LDL_CHOLESTEROL', 'HDL_CHOLESTEROL', 'TRIGLYCERIDES', 'TOTAL_CHOLESTEROL')[explode_idx],
                                            -- lab_result_qual
    NULL,                                   -- lab_result_abnorm_flg
    NULL,                                   -- lab_result_norm_min_msrmt
    NULL,                                   -- lab_result_norm_max_msrmt
    NULL,                                   -- lab_test_diag_cd
    NULL,                                   -- lab_test_diag_cd_qual
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'lipidpanel',                           -- prmy_src_tbl_nm
    extract_date(
        substring(lip.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM lipidpanel lip
    LEFT JOIN demographics_local dem ON lip.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND lip.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(lip.encounterdate, 1, 8),
                substring(lip.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(lip.encounterdate, 1, 8),
                substring(lip.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3)) as explode_idx) x
WHERE
    ARRAY(lip.ldl, lip.hdl, lip.triglycerides,
            lip.totalcholesterol)[explode_idx]  IS NOT NULL
DISTRIBUTE BY hvid;
