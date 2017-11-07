INSERT INTO encounter_common_model
SELECT
    NULL,                                   -- row_id
    concat_ws('_', '35',
        enc.reportingenterpriseid,
        enc.encounterid),                   -- hv_enc_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    enc.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    enc.reportingenterpriseid,              -- vdr_org_id
    enc.encounterid,                        -- vdr_enc_id
    CASE WHEN enc.encounterid IS NOT NULL THEN 'VENDOR'
        END,                                -- vdr_enc_id_qual
    NULL,                                   -- vdr_alt_enc_id
    NULL,                                   -- vdr_alt_enc_id_qual
    concat_ws('_', 'NG',
        enc.reportingenterpriseid,
        enc.nextgengroupid),                -- hvid
    dem.birthyear,                          -- ptnt_birth_yr
    NULL,                                   -- ptnt_age_num
    NULL,                                   -- ptnt_lvg_flg
    NULL,                                   -- ptnt_dth_dt
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END,                       -- ptnt_gender_cd
    NULL,                                   -- ptnt_state_cd
    dem.zip3,                               -- ptnt_zip3_cd
    extract_date(
        substring(enc.encounterdatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_start_dt
    NULL,                                   -- enc_end_dt
    NULL,                                   -- enc_vst_typ_cd
    NULL,                                   -- enc_rndrg_fclty_npi
    NULL,                                   -- enc_rndrg_fclty_vdr_id
    NULL,                                   -- enc_rndrg_fclty_vdr_id_qual
    NULL,                                   -- enc_rndrg_fclty_alt_id
    NULL,                                   -- enc_rndrg_fclty_alt_id_qual
    NULL,                                   -- enc_rndrg_fclty_tax_id
    NULL,                                   -- enc_rndrg_fclty_dea_id
    NULL,                                   -- enc_rndrg_fclty_state_lic_id
    NULL,                                   -- enc_rndrg_fclty_comrcl_id
    NULL,                                   -- enc_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- enc_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- enc_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- enc_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- enc_rndrg_fclty_alt_speclty_id
    NULL,                                   -- enc_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- enc_rndrg_fclty_nm
    NULL,                                   -- enc_rndrg_fclty_addr_1_txt
    NULL,                                   -- enc_rndrg_fclty_addr_2_txt
    NULL,                                   -- enc_rndrg_fclty_state_cd
    NULL,                                   -- enc_rndrg_fclty_zip_cd
    NULL,                                   -- enc_rndrg_prov_npi
    NULL,                                   -- enc_rndrg_prov_vdr_id
    NULL,                                   -- enc_rndrg_prov_vdr_id_qual
    NULL,                                   -- enc_rndrg_prov_alt_id
    NULL,                                   -- enc_rndrg_prov_alt_id_qual
    NULL,                                   -- enc_rndrg_prov_tax_id
    NULL,                                   -- enc_rndrg_prov_dea_id
    NULL,                                   -- enc_rndrg_prov_state_lic_id
    NULL,                                   -- enc_rndrg_prov_comrcl_id
    NULL,                                   -- enc_rndrg_prov_upin
    NULL,                                   -- enc_rndrg_prov_ssn
    enc.hcpprimarytaxonomy,                 -- enc_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- enc_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- enc_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- enc_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- enc_rndrg_prov_alt_speclty_id
    NULL,                                   -- enc_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- enc_rndrg_prov_frst_nm
    NULL,                                   -- enc_rndrg_prov_last_nm
    NULL,                                   -- enc_rndrg_prov_addr_1_txt
    NULL,                                   -- enc_rndrg_prov_addr_2_txt
    NULL,                                   -- enc_rndrg_prov_state_cd
    enc.hcpzipcode,                         -- enc_rndrg_prov_zip_cd
    NULL,                                   -- enc_rfrg_prov_npi
    NULL,                                   -- enc_rfrg_prov_vdr_id
    NULL,                                   -- enc_rfrg_prov_vdr_id_qual
    NULL,                                   -- enc_rfrg_prov_alt_id
    NULL,                                   -- enc_rfrg_prov_alt_id_qual
    NULL,                                   -- enc_rfrg_prov_tax_id
    NULL,                                   -- enc_rfrg_prov_dea_id
    NULL,                                   -- enc_rfrg_prov_state_lic_id
    NULL,                                   -- enc_rfrg_prov_comrcl_id
    NULL,                                   -- enc_rfrg_prov_upin
    NULL,                                   -- enc_rfrg_prov_ssn
    NULL,                                   -- enc_rfrg_prov_nucc_taxnmy_cd
    NULL,                                   -- enc_rfrg_prov_alt_taxnmy_id
    NULL,                                   -- enc_rfrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- enc_rfrg_prov_mdcr_speclty_cd
    NULL,                                   -- enc_rfrg_prov_alt_speclty_id
    NULL,                                   -- enc_rfrg_prov_alt_speclty_id_qual
    NULL,                                   -- enc_rfrg_prov_fclty_nm
    NULL,                                   -- enc_rfrg_prov_frst_nm
    NULL,                                   -- enc_rfrg_prov_last_nm
    NULL,                                   -- enc_rfrg_prov_addr_1_txt
    NULL,                                   -- enc_rfrg_prov_addr_2_txt
    NULL,                                   -- enc_rfrg_prov_state_cd
    NULL,                                   -- enc_rfrg_prov_zip_cd
    NULL,                                   -- enc_typ_cd
    NULL,                                   -- enc_typ_cd_qual
    clean_up_freetext(enc.encounterdescription),
                                            -- enc_typ_nm
    NULL,                                   -- enc_desc
    NULL,                                   -- enc_pos_cd
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'encounter'                             -- prmy_src_tbl_nm
FROM encounter_dedup enc
    LEFT JOIN demographics_local dem ON enc.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND enc.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(enc.encounterdatetime, 1, 8),
                substring(enc.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(enc.encounterdatetime, 1, 8),
                substring(enc.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL);
