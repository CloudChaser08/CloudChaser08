INSERT INTO procedure_common_model
SELECT
    NULL,                                              -- rec_id
    CONCAT('31_disp_', disp.id),                       -- hv_proc_id
    NULL,                                              -- crt_dt
    '01',                                              -- mdl_vrsn_num
    NULL,                                              -- data_set_nm
    NULL,                                              -- src_vrsn_id
    NULL,                                              -- hvm_vdr_id
    NULL,                                              -- hvm_vdr_feed_id
    NULL,                                              -- vdr_org_id
    CONCAT('disp_', disp.id),                          -- vdr_proc_id
    NULL,                                              -- vdr_proc_id_qual
    dem.patient_id,                                    -- hvid
    dem.birth_date,                                    -- ptnt_birth_yr
    dem.patient_age,                                   -- ptnt_age_num
    CASE
    WHEN lower(dem.patient_alive_indicator) IN ('n', 'no') THEN 'N'
    WHEN lower(dem.patient_alive_indicator) IN ('y', 'yes') THEN 'Y'
    ELSE NULL
    END,                                               -- ptnt_lvg_flg
    SUBSTRING(CAST(dem.date_of_death AS DATE), 0, 7),  -- ptnt_dth_dt
    dem.gender,                                        -- ptnt_gender_cd
    UPPER(dem.state),                                  -- ptnt_state_cd
    dem.zip_code,                                      -- ptnt_zip3_cd
    NULL,                                              -- hv_enc_id
    NULL,                                              -- enc_dt
    EXTRACT_DATE(
        disp.admin_date
        '%Y-%m-%d %H:%M:%S.%f',
        CAST({min_date} AS DATE)
        CAST({max_date} AS DATE)
        ),                                             -- proc_dt
    NULL,                                              -- proc_rndrg_fclty_npi
    disp.practice_id,                                  -- proc_rndrg_fclty_vdr_id
    NULL,                                              -- proc_rndrg_fclty_vdr_id_qual
    NULL,                                              -- proc_rndrg_fclty_alt_id
    NULL,                                              -- proc_rndrg_fclty_alt_id_qual
    NULL,                                              -- proc_rndrg_fclty_tax_id
    NULL,                                              -- proc_rndrg_fclty_dea_id
    NULL,                                              -- proc_rndrg_fclty_state_lic_id
    NULL,                                              -- proc_rndrg_fclty_comrcl_id
    NULL,                                              -- proc_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                              -- proc_rndrg_fclty_alt_taxnmy_id
    NULL,                                              -- proc_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                              -- proc_rndrg_fclty_mdcr_speclty_cd
    NULL,                                              -- proc_rndrg_fclty_alt_speclty_id
    NULL,                                              -- proc_rndrg_fclty_alt_speclty_id_qual
    NULL,                                              -- proc_rndrg_fclty_fclty_nm
    NULL,                                              -- proc_rndrg_fclty_addr_1_txt
    NULL,                                              -- proc_rndrg_fclty_addr_2_txt
    NULL,                                              -- proc_rndrg_fclty_state_cd
    NULL,                                              -- proc_rndrg_fclty_zip_cd
    disp.npi,                                          -- proc_rndrg_prov_npi
    NULL,                                              -- proc_rndrg_prov_vdr_id
    NULL,                                              -- proc_rndrg_prov_vdr_id_qual
    NULL,                                              -- proc_rndrg_prov_alt_id
    NULL,                                              -- proc_rndrg_prov_alt_id_qual
    NULL,                                              -- proc_rndrg_prov_tax_id
    NULL,                                              -- proc_rndrg_prov_dea_id
    NULL,                                              -- proc_rndrg_prov_state_lic_id
    NULL,                                              -- proc_rndrg_prov_comrcl_id
    NULL,                                              -- proc_rndrg_prov_upin
    NULL,                                              -- proc_rndrg_prov_ssn
    NULL,                                              -- proc_rndrg_prov_nucc_taxnmy_cd
    NULL,                                              -- proc_rndrg_prov_alt_taxnmy_id
    NULL,                                              -- proc_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                              -- proc_rndrg_prov_mdcr_speclty_cd
    NULL,                                              -- proc_rndrg_prov_alt_speclty_id
    NULL,                                              -- proc_rndrg_prov_alt_speclty_id_qual
    NULL,                                              -- proc_rndrg_prov_frst_nm
    NULL,                                              -- proc_rndrg_prov_last_nm
    NULL,                                              -- proc_rndrg_prov_addr_1_txt
    NULL,                                              -- proc_rndrg_prov_addr_2_txt
    NULL,                                              -- proc_rndrg_prov_state_cd
    NULL,                                              -- proc_rndrg_prov_zip_cd
    disp.cpt,                                          -- proc_cd
    NULL,                                              -- proc_cd_qual
    NULL,                                              -- proc_cd_1_modfr
    NULL,                                              -- proc_cd_2_modfr
    NULL,                                              -- proc_cd_3_modfr
    NULL,                                              -- proc_cd_4_modfr
    NULL,                                              -- proc_cd_modfr_qual
    NULL,                                              -- proc_snomed_cd
    NULL,                                              -- proc_prty_cd
    NULL,                                              -- proc_prty_cd_qual
    NULL,                                              -- proc_alt_cd
    NULL,                                              -- proc_alt_cd_qual
    NULL,                                              -- proc_pos_cd
    NULL,                                              -- proc_unit_qty
    NULL,                                              -- proc_uom
    NULL,                                              -- proc_diag_cd
    NULL,                                              -- proc_diag_cd_qual
    NULL,                                              -- data_captr_dt
    NULL,                                              -- rec_stat_cd
    'dispense'                                         -- prmy_src_tbl_nm
FROM (
    SELECT patient_id, SUBSTRING(admin_date, 0, 10) as admin_date, practice_id, cpt,
        npi, max(id) as id
    FROM dispense_transactions
    GROUP BY 1, 2, 3, 4, 5
        ) disp
    LEFT JOIN demographics_transactions dem ON disp.patient_id = dem.patient_id
;
