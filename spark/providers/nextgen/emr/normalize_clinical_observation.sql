INSERT INTO clinical_observation_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_clin_obsn_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    sub.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    sub.reportingenterpriseid,              -- vdr_org_id
    NULL,                                   -- vdr_clin_obsn_id
    NULL,                                   -- vdr_clin_obsn_id_qual
    concat_ws('_', 'NG',
        sub.reportingenterpriseid,
        sub.nextgengroupid),                -- hvid
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
        sub.reportingenterpriseid,
        sub.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(sub.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(sub.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- clin_obsn_dt
    NULL,                                   -- clin_obsn_rndrg_fclty_npi
    NULL,                                   -- clin_obsn_rndrg_fclty_vdr_id
    NULL,                                   -- clin_obsn_rndrg_fclty_vdr_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_id
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_tax_id
    NULL,                                   -- clin_obsn_rndrg_fclty_dea_id
    NULL,                                   -- clin_obsn_rndrg_fclty_state_lic_id
    NULL,                                   -- clin_obsn_rndrg_fclty_comrcl_id
    NULL,                                   -- clin_obsn_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_speclty_id
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_nm
    NULL,                                   -- clin_obsn_rndrg_fclty_addr_1_txt
    NULL,                                   -- clin_obsn_rndrg_fclty_addr_2_txt
    NULL,                                   -- clin_obsn_rndrg_fclty_state_cd
    NULL,                                   -- clin_obsn_rndrg_fclty_zip_cd
    NULL,                                   -- clin_obsn_rndrg_prov_npi
    NULL,                                   -- clin_obsn_rndrg_prov_vdr_id
    NULL,                                   -- clin_obsn_rndrg_prov_vdr_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_alt_id
    NULL,                                   -- clin_obsn_rndrg_prov_alt_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_tax_id
    NULL,                                   -- clin_obsn_rndrg_prov_dea_id
    NULL,                                   -- clin_obsn_rndrg_prov_state_lic_id
    NULL,                                   -- clin_obsn_rndrg_prov_comrcl_id
    NULL,                                   -- clin_obsn_rndrg_prov_upin
    NULL,                                   -- clin_obsn_rndrg_prov_ssn
    NULL,                                   -- clin_obsn_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- clin_obsn_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- clin_obsn_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- clin_obsn_rndrg_prov_alt_speclty_id
    NULL,                                   -- clin_obsn_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_frst_nm
    NULL,                                   -- clin_obsn_rndrg_prov_last_nm
    NULL,                                   -- clin_obsn_rndrg_prov_addr_1_txt
    NULL,                                   -- clin_obsn_rndrg_prov_addr_2_txt
    NULL,                                   -- clin_obsn_rndrg_prov_state_cd
    NULL,                                   -- clin_obsn_rndrg_prov_zip_cd
    NULL,                                   -- clin_obsn_onset_dt
    NULL,                                   -- clin_obsn_resltn_dt
    NULL,                                   -- clin_obsn_data_src_cd
    NULL,                                   -- clin_obsn_data_src_cd_qual
    NULL,                                   -- clin_obsn_data_src_nm
    NULL,                                   -- clin_obsn_data_src_desc
    NULL,                                   -- clin_obsn_data_ctgy_cd
    NULL,                                   -- clin_obsn_data_ctgy_cd_qual
    NULL,                                   -- clin_obsn_data_ctgy_nm
    NULL,                                   -- clin_obsn_data_ctgy_desc
    ref2.gen_ref_cd,                        -- clin_obsn_typ_cd
    CASE WHEN ref2.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END,                                -- clin_obsn_typ_cd_qual
    ref3.gen_ref_itm_nm,                    -- clin_obsn_typ_nm
    NULL,                                   -- clin_obsn_typ_desc
    ref1.gen_ref_cd,                        -- clin_obsn_substc_cd
    CASE WHEN ref1.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END,                                -- clin_obsn_substc_cd_qual
    ref1.gen_ref_itm_nm,                    -- clin_obsn_substc_nm
    NULL,                                   -- clin_obsn_substc_desc
    NULL,                                   -- clin_obsn_cd
    NULL,                                   -- clin_obsn_cd_qual
    CASE WHEN CAST(sub.emrcode AS DOUBLE) IS NOT NULL THEN sub.emrcode
        ELSE ref4.gen_ref_itm_nm END,       -- clin_obsn_nm
    NULL,                                   -- clin_obsn_desc
    NULL,                                   -- clin_obsn_diag_cd
    NULL,                                   -- clin_obsn_diag_cd_qual
    NULL,                                   -- clin_obsn_diag_nm
    NULL,                                   -- clin_obsn_diag_desc
    NULL,                                   -- clin_obsn_snomed_cd
    NULL,                                   -- clin_obsn_result_cd
    NULL,                                   -- clin_obsn_result_cd_qual
    NULL,                                   -- clin_obsn_result_nm
    NULL,                                   -- clin_obsn_result_desc
    NULL,                                   -- clin_obsn_msrmt
    NULL,                                   -- clin_obsn_uom
    NULL,                                   -- clin_obsn_qual
    NULL,                                   -- clin_obsn_abnorm_flg
    NULL,                                   -- clin_obsn_norm_min_msrmt
    NULL,                                   -- clin_obsn_norm_max_msrmt
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'substanceusage',                       -- prmy_src_tbl_nm
    extract_date(
        substring(sub.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM substanceusage sub
    LEFT JOIN demographics_local dem ON sub.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND sub.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(sub.encounterdate, 1, 8),
                substring(sub.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(sub.encounterdate, 1, 8),
                substring(sub.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'substanceusage.substancecode'
        AND sub.substancecode = ref1.gen_ref_cd
        AND ref1.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref2 ON ref2.hvm_vdr_feed_id = 35
        AND ref2.gen_ref_domn_nm = 'substanceusage.clinicalrecordtypecode'
        AND clean_up_freetext(sub.clinicalrecordtypecode, false) = ref2.gen_ref_cd
        AND ref2.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref3 ON ref3.hvm_vdr_feed_id = 35
        AND ref3.gen_ref_domn_nm = 'substanceusage.clinicalrecorddescription'
        AND clean_up_freetext(sub.clinicalrecorddescription, false) = ref3.gen_ref_itm_nm
        AND ref3.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref4 ON ref4.gen_ref_domn_nm = 'emr_clin_obsn.clin_obsn_nm'
        AND TRIM(UPPER(sub.emrcode)) = ref4.gen_ref_itm_nm
        AND ref4.whtlst_flg = 'Y';


-- ALLERGY DATA IS NOT YET CERTIFIED
--INSERT INTO clinical_observation_common_model
--SELECT
--    NULL,                                   -- row_id
--    NULL,                                   -- hv_clin_obsn_id
--    NULL,                                   -- crt_dt
--    '04',                                   -- mdl_vrsn_num
--    agy.dataset,                            -- data_set_nm
--    NULL,                                   -- src_vrsn_id
--    NULL,                                   -- hvm_vdr_id
--    NULL,                                   -- hvm_vdr_feed_id
--    agy.reportingenterpriseid,              -- vdr_org_id
--    NULL,                                   -- vdr_clin_obsn_id
--    NULL,                                   -- vdr_clin_obsn_id_qual
--    concat_ws('_', 'NG',
--        agy.reportingenterpriseid,
--        agy.nextgengroupid),                -- hvid
--    dem.birthyear,                          -- ptnt_birth_yr
--    NULL,                                   -- ptnt_age_num
--    NULL,                                   -- ptnt_lvg_flg
--    NULL,                                   -- ptnt_dth_dt
--    CASE WHEN dem.gender = 'M' THEN 'M'
--        WHEN dem.gender = 'F' THEN 'F'
--        ELSE 'U' END,                       -- ptnt_gender_cd
--    NULL,                                   -- ptnt_state_cd
--    dem.zip3,                               -- ptnt_zip3_cd
--    concat_ws('_', '35',
--        agy.reportingenterpriseid,
--        agy.encounter_id),                  -- hv_enc_id
--    extract_date(
--        substring(agy.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
--        ),                                  -- enc_dt
--    NULL,                                   -- clin_obsn_dt
--    NULL,                                   -- clin_obsn_rndrg_fclty_npi
--    NULL,                                   -- clin_obsn_rndrg_fclty_vdr_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_vdr_id_qual
--    NULL,                                   -- clin_obsn_rndrg_fclty_alt_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_alt_id_qual
--    NULL,                                   -- clin_obsn_rndrg_fclty_tax_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_dea_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_state_lic_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_comrcl_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_nucc_taxnmy_cd
--    NULL,                                   -- clin_obsn_rndrg_fclty_alt_taxnmy_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_alt_taxnmy_id_qual
--    NULL,                                   -- clin_obsn_rndrg_fclty_mdcr_speclty_cd
--    NULL,                                   -- clin_obsn_rndrg_fclty_alt_speclty_id
--    NULL,                                   -- clin_obsn_rndrg_fclty_alt_speclty_id_qual
--    NULL,                                   -- clin_obsn_rndrg_fclty_nm
--    NULL,                                   -- clin_obsn_rndrg_fclty_addr_1_txt
--    NULL,                                   -- clin_obsn_rndrg_fclty_addr_2_txt
--    NULL,                                   -- clin_obsn_rndrg_fclty_state_cd
--    NULL,                                   -- clin_obsn_rndrg_fclty_zip_cd
--    NULL,                                   -- clin_obsn_rndrg_prov_npi
--    NULL,                                   -- clin_obsn_rndrg_prov_vdr_id
--    NULL,                                   -- clin_obsn_rndrg_prov_vdr_id_qual
--    NULL,                                   -- clin_obsn_rndrg_prov_alt_id
--    NULL,                                   -- clin_obsn_rndrg_prov_alt_id_qual
--    NULL,                                   -- clin_obsn_rndrg_prov_tax_id
--    NULL,                                   -- clin_obsn_rndrg_prov_dea_id
--    NULL,                                   -- clin_obsn_rndrg_prov_state_lic_id
--    NULL,                                   -- clin_obsn_rndrg_prov_comrcl_id
--    NULL,                                   -- clin_obsn_rndrg_prov_upin
--    NULL,                                   -- clin_obsn_rndrg_prov_ssn
--    NULL,                                   -- clin_obsn_rndrg_prov_nucc_taxnmy_cd
--    NULL,                                   -- clin_obsn_rndrg_prov_alt_taxnmy_id
--    NULL,                                   -- clin_obsn_rndrg_prov_alt_taxnmy_id_qual
--    NULL,                                   -- clin_obsn_rndrg_prov_mdcr_speclty_cd
--    NULL,                                   -- clin_obsn_rndrg_prov_alt_speclty_id
--    NULL,                                   -- clin_obsn_rndrg_prov_alt_speclty_id_qual
--    NULL,                                   -- clin_obsn_rndrg_prov_frst_nm
--    NULL,                                   -- clin_obsn_rndrg_prov_last_nm
--    NULL,                                   -- clin_obsn_rndrg_prov_addr_1_txt
--    NULL,                                   -- clin_obsn_rndrg_prov_addr_2_txt
--    NULL,                                   -- clin_obsn_rndrg_prov_state_cd
--    NULL,                                   -- clin_obsn_rndrg_prov_zip_cd
--    extract_date(
--        substring(agy.onsetdate, 1, 8), '%Y%m%d', NULL, CAST({max_date} AS DATE)
--        ),                                  -- clin_obsn_onset_dt
--    extract_date(
--        substring(agy.resolveddate, 1, 8), '%Y%m%d', NULL, CAST({max_date} AS DATE)
--        ),                                  -- clin_obsn_resltn_dt
--    NULL,                                   -- clin_obsn_data_src_cd
--    NULL,                                   -- clin_obsn_data_src_cd_qual
--    NULL,                                   -- clin_obsn_data_src_nm
--    NULL,                                   -- clin_obsn_data_src_desc
--    NULL,                                   -- clin_obsn_data_ctgy_cd
--    NULL,                                   -- clin_obsn_data_ctgy_cd_qual
--    NULL,                                   -- clin_obsn_data_ctgy_nm
--    NULL,                                   -- clin_obsn_data_ctgy_desc
--    agy.allergentype,                       -- clin_obsn_typ_cd
--    'ALLERGEN_TYPE_CODE',                   -- clin_obsn_typ_cd_qual
--    ref2.gen_ref_itm_nm,                    -- clin_obsn_typ_nm
--    agy.allergentypedescription,            -- clin_obsn_typ_desc
--    agy.allergencode,                       -- clin_obsn_substc_cd
--    'ALLERGEN_CODE',                        -- clin_obsn_substc_cd_qual
--    ref1.gen_ref_itm_nm,                    -- clin_obsn_substc_nm
--    NULL,                                   -- clin_obsn_substc_desc
--    NULL,                                   -- clin_obsn_cd
--    NULL,                                   -- clin_obsn_cd_qual
--    NULL,                                   -- clin_obsn_nm
--    agy.reportedsymptoms,                   -- clin_obsn_desc
--    NULL,                                   -- clin_obsn_diag_cd
--    NULL,                                   -- clin_obsn_diag_cd_qual
--    NULL,                                   -- clin_obsn_diag_nm
--    NULL,                                   -- clin_obsn_diag_desc
--    NULL,                                   -- clin_obsn_snomed_cd
--    agy.intolerenceind,                     -- clin_obsn_result_cd
--    'EXTREME_REACTION_INDICATOR',           -- clin_obsn_result_cd_qual
--    NULL,                                   -- clin_obsn_result_nm
--    NULL,                                   -- clin_obsn_result_desc
--    NULL,                                   -- clin_obsn_msrmt
--    NULL,                                   -- clin_obsn_uom
--    NULL,                                   -- clin_obsn_qual
--    NULL,                                   -- clin_obsn_abnorm_flg
--    NULL,                                   -- clin_obsn_norm_min_msrmt
--    NULL,                                   -- clin_obsn_norm_max_msrmt
--    NULL,                                   -- data_captr_dt
--    NULL,                                   -- rec_stat_cd
--    'allergy',                              -- prmy_src_tbl_nm
--    extract_date(
--        substring(agy.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
--        )                                   -- part_mth
--FROM allergy agy
--    LEFT JOIN demographics_dedup dem ON agy.ReportingEnterpriseID = dem.ReportingEnterpriseID
--        AND agy.NextGenGroupID = dem.NextGenGroupID
--    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
--        AND ref1.gen_ref_domn_nm = 'allergy.allergencode'
--        AND agy.allergencode = ref1.gen_ref_cd
--    LEFT JOIN ref_gen_ref ref2 ON ref2.hvm_vdr_feed_id = 35
--        AND ref2.gen_ref_domn_nm = 'allergy.allergentype'
--        AND agy.allergentype = ref2.gen_ref_cd

INSERT INTO clinical_observation_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_clin_obsn_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    ext.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    ext.reportingenterpriseid,              -- vdr_org_id
    NULL,                                   -- vdr_clin_obsn_id
    NULL,                                   -- vdr_clin_obsn_id_qual
    concat_ws('_', 'NG',
        ext.reportingenterpriseid,
        ext.nextgengroupid),                -- hvid
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
        ext.reportingenterpriseid,
        ext.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(ext.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(ext.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- clin_obsn_dt
    NULL,                                   -- clin_obsn_rndrg_fclty_npi
    NULL,                                   -- clin_obsn_rndrg_fclty_vdr_id
    NULL,                                   -- clin_obsn_rndrg_fclty_vdr_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_id
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_tax_id
    NULL,                                   -- clin_obsn_rndrg_fclty_dea_id
    NULL,                                   -- clin_obsn_rndrg_fclty_state_lic_id
    NULL,                                   -- clin_obsn_rndrg_fclty_comrcl_id
    NULL,                                   -- clin_obsn_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_speclty_id
    NULL,                                   -- clin_obsn_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- clin_obsn_rndrg_fclty_nm
    NULL,                                   -- clin_obsn_rndrg_fclty_addr_1_txt
    NULL,                                   -- clin_obsn_rndrg_fclty_addr_2_txt
    NULL,                                   -- clin_obsn_rndrg_fclty_state_cd
    NULL,                                   -- clin_obsn_rndrg_fclty_zip_cd
    NULL,                                   -- clin_obsn_rndrg_prov_npi
    NULL,                                   -- clin_obsn_rndrg_prov_vdr_id
    NULL,                                   -- clin_obsn_rndrg_prov_vdr_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_alt_id
    NULL,                                   -- clin_obsn_rndrg_prov_alt_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_tax_id
    NULL,                                   -- clin_obsn_rndrg_prov_dea_id
    NULL,                                   -- clin_obsn_rndrg_prov_state_lic_id
    NULL,                                   -- clin_obsn_rndrg_prov_comrcl_id
    NULL,                                   -- clin_obsn_rndrg_prov_upin
    NULL,                                   -- clin_obsn_rndrg_prov_ssn
    NULL,                                   -- clin_obsn_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- clin_obsn_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- clin_obsn_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- clin_obsn_rndrg_prov_alt_speclty_id
    NULL,                                   -- clin_obsn_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- clin_obsn_rndrg_prov_frst_nm
    NULL,                                   -- clin_obsn_rndrg_prov_last_nm
    NULL,                                   -- clin_obsn_rndrg_prov_addr_1_txt
    NULL,                                   -- clin_obsn_rndrg_prov_addr_2_txt
    NULL,                                   -- clin_obsn_rndrg_prov_state_cd
    NULL,                                   -- clin_obsn_rndrg_prov_zip_cd
    NULL,                                   -- clin_obsn_onset_dt
    NULL,                                   -- clin_obsn_resltn_dt
    NULL,                                   -- clin_obsn_data_src_cd
    NULL,                                   -- clin_obsn_data_src_cd_qual
    NULL,                                   -- clin_obsn_data_src_nm
    NULL,                                   -- clin_obsn_data_src_desc
    ref2.gen_ref_cd,                        -- clin_obsn_data_ctgy_cd
    CASE WHEN ref2.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END,                                -- clin_obsn_data_ctgy_cd_qual
    ref2.gen_ref_itm_nm,                    -- clin_obsn_data_ctgy_nm
    NULL,                                   -- clin_obsn_data_ctgy_desc
    ref3.gen_ref_cd,                        -- clin_obsn_typ_cd
    CASE WHEN ref3.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END,                                -- clin_obsn_typ_cd_qual
    ref4.gen_ref_itm_nm,                    -- clin_obsn_typ_nm
    NULL,                                   -- clin_obsn_typ_desc
    NULL,                                   -- clin_obsn_substc_cd
    NULL,                                   -- clin_obsn_substc_cd_qual
    NULL,                                   -- clin_obsn_substc_nm
    NULL,                                   -- clin_obsn_substc_desc
    NULL,                                   -- clin_obsn_cd
    NULL,                                   -- clin_obsn_cd_qual
    CASE WHEN CAST(ext.emrcode AS DOUBLE) IS NOT NULL THEN ext.emrcode
        ELSE ref5.gen_ref_itm_nm END,       -- clin_obsn_nm
    NULL,                                   -- clin_obsn_desc
    NULL,                                   -- clin_obsn_diag_cd
    NULL,                                   -- clin_obsn_diag_cd_qual
    NULL,                                   -- clin_obsn_diag_nm
    NULL,                                   -- clin_obsn_diag_desc
    NULL,                                   -- clin_obsn_snomed_cd
    NULL,                                   -- clin_obsn_result_cd
    NULL,                                   -- clin_obsn_result_cd_qual
    NULL,                                   -- clin_obsn_result_nm
    CASE WHEN CAST(ext.result AS DOUBLE) IS NOT NULL THEN ext.result
        ELSE ref6.gen_ref_itm_nm END,       -- clin_obsn_result_desc
    NULL,                                   -- clin_obsn_msrmt
    NULL,                                   -- clin_obsn_uom
    NULL,                                   -- clin_obsn_qual
    NULL,                                   -- clin_obsn_abnorm_flg
    NULL,                                   -- clin_obsn_norm_min_msrmt
    NULL,                                   -- clin_obsn_norm_max_msrmt
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'extendeddata',                         -- prmy_src_tbl_nm
    extract_date(
        substring(ext.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM extendeddata ext
    LEFT JOIN demographics_local dem ON ext.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND ext.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(ext.encounterdate, 1, 8),
                substring(ext.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(ext.encounterdate, 1, 8),
                substring(ext.referencedatetime, 1, 8)
            ) <= substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref2 ON ref2.hvm_vdr_feed_id = 35
        AND ref2.gen_ref_domn_nm = 'extendeddata.datacategory'
        AND ext.datacategory = ref2.gen_ref_cd
        AND ref2.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref3 ON ref3.hvm_vdr_feed_id = 35
        AND ref3.gen_ref_domn_nm = 'extendeddata.clinicalrecordtypecode'
        AND clean_up_freetext(ext.clinicalrecordtypecode, false) = ref3.gen_ref_cd
        AND ref3.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref4 ON ref4.hvm_vdr_feed_id = 35
        AND ref4.gen_ref_domn_nm = 'extendeddata.clinicalrecorddescription'
        AND clean_up_freetext(ext.clinicalrecorddescription, false) = ref4.gen_ref_itm_nm
        AND ref4.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref5 ON ref5.gen_ref_domn_nm = 'emr_clin_obsn.clin_obsn_nm'
        AND TRIM(UPPER(ext.emrcode)) = ref5.gen_ref_itm_nm
        AND ref5.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref6 ON ref6.gen_ref_domn_nm = 'emr_clin_obsn.clin_obsn_result_desc'
        AND TRIM(UPPER(ext.result)) = ref6.gen_ref_itm_nm
        AND ref6.whtlst_flg = 'Y';
