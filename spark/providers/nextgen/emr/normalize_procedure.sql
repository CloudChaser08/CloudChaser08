INSERT INTO procedure_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_proc_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    proc.dataset,                           -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    proc.reportingenterpriseid,             -- vdr_org_id
    NULL,                                   -- vdr_proc_id
    NULL,                                   -- vdr_proc_id_qual
    concat_ws('_', 'NG',
        proc.reportingenterpriseid,
        proc.nextgengroupid),               -- hvid
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
        proc.reportingenterpriseid,
        proc.encounter_id),                 -- hv_enc_id
    extract_date(
        substring(proc.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(proc.datadatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- proc_dt
    NULL,                                   -- proc_rndrg_fclty_npi
    NULL,                                   -- proc_rndrg_fclty_vdr_id
    NULL,                                   -- proc_rndrg_fclty_vdr_id_qual
    NULL,                                   -- proc_rndrg_fclty_alt_id
    NULL,                                   -- proc_rndrg_fclty_alt_id_qual
    NULL,                                   -- proc_rndrg_fclty_tax_id
    NULL,                                   -- proc_rndrg_fclty_dea_id
    NULL,                                   -- proc_rndrg_fclty_state_lic_id
    NULL,                                   -- proc_rndrg_fclty_comrcl_id
    NULL,                                   -- proc_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- proc_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- proc_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- proc_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- proc_rndrg_fclty_alt_speclty_id
    NULL,                                   -- proc_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- proc_rndrg_fclty_nm
    NULL,                                   -- proc_rndrg_fclty_addr_1_txt
    NULL,                                   -- proc_rndrg_fclty_addr_2_txt
    NULL,                                   -- proc_rndrg_fclty_state_cd
    NULL,                                   -- proc_rndrg_fclty_zip_cd
    NULL,                                   -- proc_rndrg_prov_npi
    NULL,                                   -- proc_rndrg_prov_vdr_id
    NULL,                                   -- proc_rndrg_prov_vdr_id_qual
    NULL,                                   -- proc_rndrg_prov_alt_id
    NULL,                                   -- proc_rndrg_prov_alt_id_qual
    NULL,                                   -- proc_rndrg_prov_tax_id
    NULL,                                   -- proc_rndrg_prov_dea_id
    NULL,                                   -- proc_rndrg_prov_state_lic_id
    NULL,                                   -- proc_rndrg_prov_comrcl_id
    NULL,                                   -- proc_rndrg_prov_upin
    NULL,                                   -- proc_rndrg_prov_ssn
    NULL,                                   -- proc_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- proc_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- proc_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- proc_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- proc_rndrg_prov_alt_speclty_id
    NULL,                                   -- proc_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- proc_rndrg_prov_frst_nm
    NULL,                                   -- proc_rndrg_prov_last_nm
    NULL,                                   -- proc_rndrg_prov_addr_1_txt
    NULL,                                   -- proc_rndrg_prov_addr_2_txt
    NULL,                                   -- proc_rndrg_prov_state_cd
    NULL,                                   -- proc_rndrg_prov_zip_cd
    proc.emrcode,                           -- proc_cd
    CASE WHEN proc.emrcode IS NOT NULL THEN 'CPT'
        END,                                -- proc_cd_qual
    NULL,                                   -- proc_cd_1_modfr
    NULL,                                   -- proc_cd_2_modfr
    NULL,                                   -- proc_cd_3_modfr
    NULL,                                   -- proc_cd_4_modfr
    NULL,                                   -- proc_cd_modfr_qual
    NULL,                                   -- proc_snomed_cd
    NULL,                                   -- proc_prty_cd
    NULL,                                   -- proc_prty_cd_qual
    NULL,                                   -- proc_alt_cd
    NULL,                                   -- proc_alt_cd_qual
    NULL,                                   -- proc_pos_cd
    NULL,                                   -- proc_unit_qty
    NULL,                                   -- proc_uom
    NULL,                                   -- proc_diag_cd
    NULL,                                   -- proc_diag_cd_qual
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'procedure'                             -- prmy_src_tbl_nm
FROM `procedure` proc
    LEFT JOIN demographics_local dem ON proc.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND proc.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(proc.encounterdate, 1, 8),
                substring(proc.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(proc.encounterdate, 1, 8),
                substring(proc.referencedatetime, 1, 8)
            ) <= substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL);
