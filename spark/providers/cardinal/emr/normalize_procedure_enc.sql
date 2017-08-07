INSERT INTO procedure_common_model
    NULL,                                            -- rec_id
    CONCAT('31_enc_', e.id),                         -- hv_proc_id
    NULL,                                            -- crt_dt
    NULL,                                            -- mdl_vrsn_num
    NULL,                                            -- data_set_nm
    NULL,                                            -- src_vrsn_id
    NULL,                                            -- hvm_vdr_id
    NULL,                                            -- hvm_vdr_feed_id
    NULL,                                            -- vdr_org_id
    CONCAT('enc_', e.id),                            -- vdr_proc_id
    NULL,                                            -- vdr_proc_id_qual
    d.patient_id,                                    -- hvid
    NULL,                                            -- ptnt_birth_yr
    NULL,                                            -- ptnt_age_num
    CASE
    WHEN lower(d.patient_alive_indicator) IN ('n', 'no') THEN 'N'
    WHEN lower(d.patient_alive_indicator) IN ('y', 'yes') THEN 'Y'
    ELSE NULL
    END,                                             -- ptnt_lvg_flg
    SUBSTRING(CAST(d.date_of_death AS DATE), 0, 7),  -- ptnt_dth_dt
    NULL,                                            -- ptnt_gender_cd
    NULL,                                            -- ptnt_state_cd
    NULL,                                            -- ptnt_zip3_cd
    CONCAT('31_', e.id),                             -- hv_enc_id
    EXTRACT_DATE(
        e.visit_date,
        '%Y-%m-%d %H:%M:%S.%f',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                           -- enc_dt
    EXTRACT_DATE(
        e.visit_date,
        '%Y-%m-%d %H:%M:%S.%f',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                           -- proc_dt
    NULL,                                            -- proc_rndrg_fclty_npi
    e.practice_id,                                   -- proc_rndrg_fclty_vdr_id
    NULL,                                            -- proc_rndrg_fclty_vdr_id_qual
    NULL,                                            -- proc_rndrg_fclty_alt_id
    NULL,                                            -- proc_rndrg_fclty_alt_id_qual
    NULL,                                            -- proc_rndrg_fclty_tax_id
    NULL,                                            -- proc_rndrg_fclty_dea_id
    NULL,                                            -- proc_rndrg_fclty_state_lic_id
    NULL,                                            -- proc_rndrg_fclty_comrcl_id
    NULL,                                            -- proc_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                            -- proc_rndrg_fclty_alt_taxnmy_id
    NULL,                                            -- proc_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                            -- proc_rndrg_fclty_mdcr_speclty_cd
    NULL,                                            -- proc_rndrg_fclty_alt_speclty_id
    NULL,                                            -- proc_rndrg_fclty_alt_speclty_id_qual
    NULL,                                            -- proc_rndrg_fclty_fclty_nm
    NULL,                                            -- proc_rndrg_fclty_addr_1_txt
    NULL,                                            -- proc_rndrg_fclty_addr_2_txt
    NULL,                                            -- proc_rndrg_fclty_state_cd
    NULL,                                            -- proc_rndrg_fclty_zip_cd
    e.provider_npi                                   -- proc_rndrg_prov_npi
    NULL,                                            -- proc_rndrg_prov_vdr_id
    NULL,                                            -- proc_rndrg_prov_vdr_id_qual
    NULL,                                            -- proc_rndrg_prov_alt_id
    NULL,                                            -- proc_rndrg_prov_alt_id_qual
    NULL,                                            -- proc_rndrg_prov_tax_id
    NULL,                                            -- proc_rndrg_prov_dea_id
    NULL,                                            -- proc_rndrg_prov_state_lic_id
    NULL,                                            -- proc_rndrg_prov_comrcl_id
    NULL,                                            -- proc_rndrg_prov_upin
    NULL,                                            -- proc_rndrg_prov_ssn
    NULL,                                            -- proc_rndrg_prov_nucc_taxnmy_cd
    NULL,                                            -- proc_rndrg_prov_alt_taxnmy_id
    NULL,                                            -- proc_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                            -- proc_rndrg_prov_mdcr_speclty_cd
    NULL,                                            -- proc_rndrg_prov_alt_speclty_id
    NULL,                                            -- proc_rndrg_prov_alt_speclty_id_qual
    NULL,                                            -- proc_rndrg_prov_frst_nm
    NULL,                                            -- proc_rndrg_prov_last_nm
    NULL,                                            -- proc_rndrg_prov_addr_1_txt
    NULL,                                            -- proc_rndrg_prov_addr_2_txt
    NULL,                                            -- proc_rndrg_prov_state_cd
    NULL,                                            -- proc_rndrg_prov_zip_cd
    e.cpt,                                           -- proc_cd
    'HC',                                            -- proc_cd_qual
    NULL,                                            -- proc_cd_1_modfr
    NULL,                                            -- proc_cd_2_modfr
    NULL,                                            -- proc_cd_3_modfr
    NULL,                                            -- proc_cd_4_modfr
    NULL,                                            -- proc_cd_modfr_qual
    NULL,                                            -- proc_snomed_cd
    NULL,                                            -- proc_prty_cd
    NULL,                                            -- proc_prty_cd_qual
    NULL,                                            -- proc_alt_cd
    NULL,                                            -- proc_alt_cd_qual
    NULL,                                            -- proc_pos_cd
    NULL,                                            -- proc_unit_qty
    NULL,                                            -- proc_uom
    NULL,                                            -- proc_diag_cd
    NULL,                                            -- proc_diag_cd_qual
    NULL,                                            -- data_captr_dt
    NULL,                                            -- rec_stat_cd
    'encounter'                                      -- prmy_src_tbl_nm
FROM (
    SELECT patient_id, SUBSTRING(visit_date, 0, 10) as visit_date,
        practice_id, cpt, provider_npi, max(id) as id
    FROM encounter_transactions
    GROUP BY 1, 2, 3, 4, 5
        ) e
    LEFT JOIN demographics_transactions d ON e.patient_id = d.patient_id
