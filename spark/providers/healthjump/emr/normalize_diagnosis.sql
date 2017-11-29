INSERT INTO diagnosis_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_diag_id
    NULL,                                   -- crt_dt
    '5',                                    -- mdl_vrsn_num
    NULL,                                   -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    NULL,                                   -- vdr_org_id
    NULL,                                   -- vdr_diag_id
    NULL,                                   -- vdr_diag_id_qual
    pay.hvid,                               -- hvid
    pay.yearOfBirth,                        -- ptnt_birth_yr
    NULL,                                   -- ptnt_age_num
    NULL,                                   -- ptnt_lvg_flg
    NULL,                                   -- ptnt_dth_dt
    CASE
      WHEN UPPER(COALESCE(pay.gender, dem.gender)) IN ('F', 'M')
      THEN UPPER(COALESCE(pay.gender, dem.gender))
      ELSE 'U'
    END,                                    -- ptnt_gender_cd
    UPPER(COALESCE(pay.state, dem.state)),  -- ptnt_state_cd
    pay.threeDigitZip,                      -- ptnt_zip3_cd
    NULL,                                   -- hv_enc_id
    NULL,                                   -- enc_dt
    CASE
      WHEN 8 = LENGTH(diag.date) AND 0 = LOCATE('/', diag.date)
      THEN EXTRACT_DATE(diag.date, '%Y%m%d')
      WHEN 10 = LENGTH(diag.date) AND 0 <> LOCATE('/', diag.date)
      THEN EXTRACT_DATE(diag.date, '%m/%d/%Y')
    END,                                    -- diag_dt
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
    NULL,                                   -- diag_onset_dt
    NULL,                                   -- diag_resltn_dt
    SPLIT(diag.diag, ' ')[0],               -- diag_cd
    NULL,                                   -- diag_cd_qual
    NULL,                                   -- diag_alt_cd
    NULL,                                   -- diag_alt_cd_qual
    NULL,                                   -- diag_nm
    NULL,                                   -- diag_desc
    NULL,                                   -- diag_prty_cd
    NULL,                                   -- diag_prty_cd_qual
    NULL,                                   -- diag_svty_cd
    NULL,                                   -- diag_svty_cd_qual
    NULL,                                   -- diag_resltn_cd
    NULL,                                   -- diag_resltn_cd_qual
    NULL,                                   -- diag_resltn_nm
    NULL,                                   -- diag_resltn_desc
    NULL,                                   -- diag_stat_cd
    NULL,                                   -- diag_stat_cd_qual
    NULL,                                   -- diag_stat_nm
    NULL,                                   -- diag_stat_desc
    NULL,                                   -- diag_snomed_cd
    NULL,                                   -- diag_meth_cd
    NULL,                                   -- diag_meth_cd_qual
    NULL,                                   -- diag_meth_nm
    NULL,                                   -- diag_meth_desc
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    NULL                                    -- prmy_src_tbl_nm
FROM diagnosis_transactions diag
    LEFT JOIN demographics_transactions dem ON diag.id = dem.demographicid
    LEFT JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
    ;
