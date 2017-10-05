INSERT INTO diagnosis_common_model
SELECT
    NULL,                                                       -- rec_id
    CONCAT('40_', diag.id),                                     -- hv_diag_id
    NULL,                                                       -- crt_dt
    '05',                                                       -- mdl_vrsn_num
    NULL,                                                       -- data_set_nm
    NULL,                                                       -- src_vrsn_id
    NULL,                                                       -- hvm_vdr_id
    NULL,                                                       -- hvm_vdr_feed_id
    NULL,                                                       -- vdr_org_id
    diag.id,                                                    -- vdr_diag_id
    NULL,                                                       -- vdr_diag_id_qual
    dem.patient_id,                                             -- hvid
    COALESCE(
        mp.yearOfBirth,
        SUBSTRING(dem.birth_date, 0, 4)
        ),                                                      -- ptnt_birth_yr
    COALESCE(
        CASE WHEN mp.age = 0 THEN NULL ELSE mp.age END,
        CASE WHEN dem.patient_age = 0 THEN NULL ELSE dem.patient_age END
        ),                                                      -- ptnt_age_num
    /* Do not load for now, uncertified
    CASE
    WHEN lower(dem.patient_alive_indicator) IN ('n', 'no') THEN 'N'
    WHEN lower(dem.patient_alive_indicator) IN ('y', 'yes') THEN 'Y'
    ELSE NULL
    END,                                                        -- ptnt_lvg_flg
    */
    NULL,                                                       -- patnt_lvg_flg
    /* Do not load for now, uncertified
    SUBSTRING(CAST(dem.date_of_death AS DATE), 0, 7),           -- ptnt_dth_dt
    */
    NULL,                                                       -- ptnt_dth_dt
    CASE
    WHEN UPPER(COALESCE(mp.gender, dem.gender)) NOT IN ('M', 'F')
    THEN 'U'
    ELSE UPPER(COALESCE(mp.gender, dem.gender))
    END,                                                        -- ptnt_gender_cd
    UPPER(COALESCE(mp.state, dem.state)),                       -- ptnt_state_cd
    COALESCE(mp.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3)),  -- ptnt_zip3_cd
    NULL,                                                       -- hv_enc_id
    NULL,                                                       -- enc_dt
    EXTRACT_DATE(
        SUBSTRING(diag.diagnosis_date, 0, 10),
        '%Y-%m-%d'
        ),                                                      -- diag_dt
    NULL,                                                       -- diag_rndrg_fclty_npi
    diag.practice_id,                                           -- diag_rndrg_fclty_vdr_id
    CASE WHEN diag.practice_id IS NOT NULL
    THEN 'VENDOR'
    END,                                                        -- diag_rndrg_fclty_vdr_id_qual
    NULL,                                                       -- diag_rndrg_fclty_alt_id
    NULL,                                                       -- diag_rndrg_fclty_alt_id_qual
    NULL,                                                       -- diag_rndrg_fclty_tax_id
    NULL,                                                       -- diag_rndrg_fclty_dea_id
    NULL,                                                       -- diag_rndrg_fclty_state_lic_id
    NULL,                                                       -- diag_rndrg_fclty_comrcl_id
    NULL,                                                       -- diag_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                                       -- diag_rndrg_fclty_alt_taxnmy_id
    NULL,                                                       -- diag_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                                       -- diag_rndrg_fclty_mdcr_speclty_cd
    NULL,                                                       -- diag_rndrg_fclty_alt_speclty_id
    NULL,                                                       -- diag_rndrg_fclty_alt_speclty_id_qual
    NULL,                                                       -- diag_rndrg_fclty_fclty_nm
    NULL,                                                       -- diag_rndrg_fclty_addr_1_txt
    NULL,                                                       -- diag_rndrg_fclty_addr_2_txt
    NULL,                                                       -- diag_rndrg_fclty_state_cd
    NULL,                                                       -- diag_rndrg_fclty_zip_cd
    diag.provider_npi,                                          -- diag_rndrg_prov_npi
    NULL,                                                       -- diag_rndrg_prov_vdr_id
    NULL,                                                       -- diag_rndrg_prov_vdr_id_qual
    NULL,                                                       -- diag_rndrg_prov_alt_id
    NULL,                                                       -- diag_rndrg_prov_alt_id_qual
    NULL,                                                       -- diag_rndrg_prov_tax_id
    NULL,                                                       -- diag_rndrg_prov_dea_id
    NULL,                                                       -- diag_rndrg_prov_state_lic_id
    NULL,                                                       -- diag_rndrg_prov_comrcl_id
    NULL,                                                       -- diag_rndrg_prov_upin
    NULL,                                                       -- diag_rndrg_prov_ssn
    NULL,                                                       -- diag_rndrg_prov_nucc_taxnmy_cd
    NULL,                                                       -- diag_rndrg_prov_alt_taxnmy_id
    NULL,                                                       -- diag_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                                       -- diag_rndrg_prov_mdcr_speclty_cd
    NULL,                                                       -- diag_rndrg_prov_alt_speclty_id
    NULL,                                                       -- diag_rndrg_prov_alt_speclty_id_qual
    NULL,                                                       -- diag_rndrg_prov_frst_nm
    NULL,                                                       -- diag_rndrg_prov_last_nm
    NULL,                                                       -- diag_rndrg_prov_addr_1_txt
    NULL,                                                       -- diag_rndrg_prov_addr_2_txt
    NULL,                                                       -- diag_rndrg_prov_state_cd
    NULL,                                                       -- diag_rndrg_prov_zip_cd
    NULL,                                                       -- diag_onset_dt
    EXTRACT_DATE(
        SUBSTRING(diag.resolution_date, 0, 10),
        '%Y-%m-%d'
        ),                                                      -- diag_resltn_dt
    diag.icd_cd,                                                -- diag_cd
    NULL,                                                       -- diag_cd_qual
    NULL,                                                       -- diag_alt_cd
    NULL,                                                       -- diag_alt_cd_qual
    diag.diagnosis_name,                                        -- diag_nm
    diag.diagnosis_desc,                                        -- diag_desc
    CASE
    WHEN diagnosis_typ REGEXP '[0-9]+' THEN diagnosis_typ
    WHEN UPPER(diagnosis_typ) = 'PRIMARY' THEN 1
    WHEN UPPER(diagnosis_typ) = 'SECONDARY' THEN 2
    END,                                                        -- diag_prty_cd
    NULL,                                                       -- diag_prty_cd_qual
    NULL,                                                       -- diag_svty_cd
    NULL,                                                       -- diag_svty_cd_qual
    NULL,                                                       -- diag_resltn_cd
    NULL,                                                       -- diag_resltn_cd_qual
    NULL,                                                       -- diag_resltn_nm
    diag.resolution_desc,                                       -- diag_resltn_desc
    NULL,                                                       -- diag_stat_cd
    NULL,                                                       -- diag_stat_cd_qual
    NULL,                                                       -- diag_stat_nm
    NULL,                                                       -- diag_stat_desc
    NULL,                                                       -- diag_snomed_cd
    NULL,                                                       -- diag_meth_cd
    NULL,                                                       -- diag_meth_cd_qual
    diag.mthd_of_diagnosis,                                     -- diag_meth_nm
    NULL,                                                       -- diag_meth_desc
    NULL,                                                       -- data_captr_dt
    NULL,                                                       -- rec_stat_cd
    'diagnosis'                                                 -- prmy_src_tbl_nm
FROM diagnosis_transactions diag
    LEFT JOIN demographics_transactions_dedup dem ON diag.patient_id = dem.patient_id
    LEFT JOIN matching_payload mp ON dem.hvJoinKey = mp.hvJoinKey
WHERE diag.import_source_id IS NOT NULL
