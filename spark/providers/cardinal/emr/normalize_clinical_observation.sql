INSERT INTO clinical_observation_common_model
SELECT
    NULL,                                                       -- rec_id
    CONCAT('31_', diag.id),                                     -- hv_clin_obsn_id
    NULL,                                                       -- crt_dt
    '02',                                                       -- mdl_vrsn_num
    NULL,                                                       -- data_set_nm
    NULL,                                                       -- src_vrsn_id
    NULL,                                                       -- hvm_vdr_id
    NULL,                                                       -- hvm_vdr_feed_id
    NULL,                                                       -- vdr_org_id
    diag.id,                                                    -- vdr_clin_obsn_id
    NULL,                                                       -- vdr_clin_obsn_id_qual
    mp.hvid,                                                    -- hvid
    COALESCE(
        mp.yearOfBirth,
        SUBSTRING(dem.birth_date, 0, 4)
        ),                                                      -- ptnt_birth_yr
    COALESCE(
        CASE WHEN mp.age = 0 THEN NULL ELSE mp.age END,
        CASE WHEN dem.patient_age = 0 THEN NULL ELSE dem.patient_age END
        ),                                                      -- ptnt_age_num
    CASE
    WHEN lower(dem.patient_alive_indicator) IN ('n', 'no') THEN 'N'
    WHEN lower(dem.patient_alive_indicator) IN ('y', 'yes') THEN 'Y'
    ELSE NULL
    END,                                                        -- ptnt_lvg_flg
    SUBSTRING(CAST(dem.date_of_death AS DATE), 0, 7),           -- ptnt_dth_dt
    UPPER(COALESCE(mp.gender, dem.gender)),                     -- ptnt_gender_cd
    UPPER(COALESCE(mp.state, dem.state)),                       -- ptnt_state_cd
    COALESCE(mp.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3)),  -- ptnt_zip3_cd
    NULL,                                                       -- hv_enc_id
    NULL,                                                       -- enc_dt
    EXTRACT_DATE(
        diag.diagnosis_date,
        '%Y-%m-%d %H:%M:%S.%f',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                                      -- clin_obsn_dt
    NULL,                                                       -- clin_obsn_rndrg_fclty_npi
    diag.practice_id,                                           -- clin_obsn_rndrg_fclty_vdr_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_vdr_id_qual
    NULL,                                                       -- clin_obsn_rndrg_fclty_alt_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_alt_id_qual
    NULL,                                                       -- clin_obsn_rndrg_fclty_tax_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_dea_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_state_lic_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_comrcl_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                                       -- clin_obsn_rndrg_fclty_alt_taxnmy_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                                       -- clin_obsn_rndrg_fclty_mdcr_speclty_cd
    NULL,                                                       -- clin_obsn_rndrg_fclty_alt_speclty_id
    NULL,                                                       -- clin_obsn_rndrg_fclty_alt_speclty_id_qual
    NULL,                                                       -- clin_obsn_rndrg_fclty_fclty_nm
    NULL,                                                       -- clin_obsn_rndrg_fclty_addr_1_txt
    NULL,                                                       -- clin_obsn_rndrg_fclty_addr_2_txt
    NULL,                                                       -- clin_obsn_rndrg_fclty_state_cd
    NULL,                                                       -- clin_obsn_rndrg_fclty_zip_cd
    diag.provider_npi,                                          -- clin_obsn_rndrg_prov_npi
    NULL,                                                       -- clin_obsn_rndrg_prov_vdr_id
    NULL,                                                       -- clin_obsn_rndrg_prov_vdr_id_qual
    NULL,                                                       -- clin_obsn_rndrg_prov_alt_id
    NULL,                                                       -- clin_obsn_rndrg_prov_alt_id_qual
    NULL,                                                       -- clin_obsn_rndrg_prov_tax_id
    NULL,                                                       -- clin_obsn_rndrg_prov_dea_id
    NULL,                                                       -- clin_obsn_rndrg_prov_state_lic_id
    NULL,                                                       -- clin_obsn_rndrg_prov_comrcl_id
    NULL,                                                       -- clin_obsn_rndrg_prov_upin
    NULL,                                                       -- clin_obsn_rndrg_prov_ssn
    NULL,                                                       -- clin_obsn_rndrg_prov_nucc_taxnmy_cd
    NULL,                                                       -- clin_obsn_rndrg_prov_alt_taxnmy_id
    NULL,                                                       -- clin_obsn_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                                       -- clin_obsn_rndrg_prov_mdcr_speclty_cd
    NULL,                                                       -- clin_obsn_rndrg_prov_alt_speclty_id
    NULL,                                                       -- clin_obsn_rndrg_prov_alt_speclty_id_qual
    NULL,                                                       -- clin_obsn_rndrg_prov_frst_nm
    NULL,                                                       -- clin_obsn_rndrg_prov_last_nm
    NULL,                                                       -- clin_obsn_rndrg_prov_addr_1_txt
    NULL,                                                       -- clin_obsn_rndrg_prov_addr_2_txt
    NULL,                                                       -- clin_obsn_rndrg_prov_state_cd
    NULL,                                                       -- clin_obsn_rndrg_prov_zip_cd
    NULL,                                                       -- clin_obsn_onset_dt
    EXTRACT_DATE(
        diag.resolution_date,
        '%Y-%m-%d %H:%M:%S.%f',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                                      -- clin_obsn_resltn_dt
    NULL,                                                       -- clin_obsn_data_src_cd
    NULL,                                                       -- clin_obsn_data_src_cd_qual
    NULL,                                                       -- clin_obsn_data_src_nm
    NULL,                                                       -- clin_obsn_data_src_desc
    NULL,                                                       -- clin_obsn_data_ctgy_cd
    NULL,                                                       -- clin_obsn_data_ctgy_cd_qual
    NULL,                                                       -- clin_obsn_data_ctgy_nm
    NULL,                                                       -- clin_obsn_data_ctgy_desc
    NULL,                                                       -- clin_obsn_typ_cd
    NULL,                                                       -- clin_obsn_typ_cd_qual
    NULL,                                                       -- clin_obsn_typ_nm
    NULL,                                                       -- clin_obsn_typ_desc
    NULL,                                                       -- clin_obsn_substc_cd
    NULL,                                                       -- clin_obsn_substc_cd_qual
    NULL,                                                       -- clin_obsn_substc_nm
    NULL,                                                       -- clin_obsn_substc_desc
    NULL,                                                       -- clin_obsn_cd
    NULL,                                                       -- clin_obsn_cd_qual
    NULL,                                                       -- clin_obsn_nm
    diag.clinical_desc,                                         -- clin_obsn_desc
    diag.icd_cd,                                                -- clin_obsn_diag_cd
    NULL,                                                       -- clin_obsn_diag_cd_qual
    diag.diagnosis_name,                                        -- clin_obsn_diag_nm
    diag.diagnosis_desc,                                        -- clin_obsn_diag_desc
    NULL,                                                       -- clin_obsn_snomed_cd
    ARRAY(
        diag.stg_crit_desc, diag.stage_of_disease, diag.cancer_stage,
        diag.cancer_stage_t, diag.cancer_stage_n, diag.cancer_stage_m
        )[n.n],                                                 -- clin_obsn_result_cd
    CASE n.n
    WHEN 0 THEN 'CANCER_STAGE_PATHOLOGY'
    WHEN 1 THEN 'DISEASE_STAGE'
    WHEN 2 THEN 'CANCER_STAGE'
    WHEN 3 THEN 'CANCER_STAGE_T'
    WHEN 4 THEN 'CANCER_STAGE_N'
    WHEN 5 THEN 'CANCER_STAGE_M'
    END,                                                        -- clin_obsn_result_cd_qual
    NULL,                                                       -- clin_obsn_result_nm
    NULL,                                                       -- clin_obsn_result_desc
    NULL,                                                       -- clin_obsn_msrmt
    NULL,                                                       -- clin_obsn_uom
    NULL,                                                       -- clin_obsn_qual
    NULL,                                                       -- clin_obsn_abnorm_flg
    NULL,                                                       -- clin_obsn_norm_min_msrmt
    NULL,                                                       -- clin_obsn_norm_max_msrmt
    NULL,                                                       -- data_captr_dt
    NULL,                                                       -- rec_stat_cd
    'diagnosis'                                                 -- prmy_src_tbl_nm
FROM diagnosis_transactions diag
    LEFT JOIN demographics_transactions_dedup dem ON diag.patient_id = dem.patient_id
    LEFT JOIN matching_payload mp ON dem.hvJoinKey = mp.hvJoinKey
    CROSS JOIN clin_obs_exploder n

-- filter out rows where all of these are null
WHERE ARRAY(
        diag.stg_crit_desc, diag.stage_of_disease, diag.cancer_stage,
        diag.cancer_stage_t, diag.cancer_stage_n, diag.cancer_stage_m
        )[n.n] IS NOT NULL
    AND diag.import_source_id IS NOT NULL
