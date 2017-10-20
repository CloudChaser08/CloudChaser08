INSERT INTO lab_result_common_model
SELECT
    NULL,                                                     -- rec_id
    CONCAT('40_', l.id),                                      -- hv_lab_result_id
    NULL,                                                     -- crt_dt
    '04',                                                     -- mdl_vrsn_num
    NULL,                                                     -- data_set_nm
    NULL,                                                     -- src_vrsn_id
    NULL,                                                     -- hvm_vdr_id
    NULL,                                                     -- hvm_vdr_feed_id
    NULL,                                                     -- vdr_org_id
    l.id,                                                     -- vdr_lab_test_id
    NULL,                                                     -- vdr_lab_test_id_qual
    NULL,                                                     -- vdr_lab_result_id
    NULL,                                                     -- vdr_lab_result_id_qual
    d.patient_id,                                             -- hvid
    COALESCE(
        mp.yearOfBirth,
        SUBSTRING(d.birth_date, 0, 4)
        ),                                                    -- ptnt_birth_yr
    COALESCE(
        CASE WHEN mp.age = 0 THEN NULL ELSE mp.age END,
        CASE WHEN d.patient_age = 0 THEN NULL ELSE d.patient_age END
        ),                                                    -- ptnt_age_num
    /* Do not load for now, uncertified
    CASE
    WHEN lower(d.patient_alive_indicator) IN ('n', 'no') THEN 'N'
    WHEN lower(d.patient_alive_indicator) IN ('y', 'yes') THEN 'Y'
    ELSE NULL
    END,                                                      -- ptnt_lvg_flg
    */
    NULL,                                                     -- patnt_lvg_flg
    /* Do not load for now, uncertified
    SUBSTRING(CAST(d.date_of_death AS DATE), 0, 7),           -- ptnt_dth_dt
    */
    NULL,                                                     -- ptnt_dth_dt
    CASE
    WHEN UPPER(COALESCE(mp.gender, d.gender)) NOT IN ('M', 'F')
    THEN 'U'
    ELSE UPPER(COALESCE(mp.gender, d.gender))
    END,                                                      -- ptnt_gender_cd
    UPPER(COALESCE(mp.state, d.state)),                       -- ptnt_state_cd
    COALESCE(mp.threeDigitZip, SUBSTRING(d.zip_code, 0, 3)),  -- ptnt_zip3_cd
    NULL,                                                     -- hv_enc_id
    NULL,                                                     -- enc_dt
    NULL,                                                     -- hv_lab_ord_id
    NULL,                                                     -- lab_test_smpl_collctn_dt
    NULL,                                                     -- lab_test_schedd_dt
    EXTRACT_DATE(
        SUBSTRING(l.test_date, 0, 10),
        '%Y-%m-%d'
        ),                                                    -- lab_test_execd_dt
    NULL,                                                     -- lab_result_dt
    NULL,                                                     -- lab_test_ordg_prov_npi
    NULL,                                                     -- lab_test_ordg_prov_vdr_id
    NULL,                                                     -- lab_test_ordg_prov_vdr_id_qual
    NULL,                                                     -- lab_test_ordg_prov_alt_id
    NULL,                                                     -- lab_test_ordg_prov_alt_id_qual
    NULL,                                                     -- lab_test_ordg_prov_tax_id
    NULL,                                                     -- lab_test_ordg_prov_dea_id
    NULL,                                                     -- lab_test_ordg_prov_state_lic_id
    NULL,                                                     -- lab_test_ordg_prov_comrcl_id
    NULL,                                                     -- lab_test_ordg_prov_upin
    NULL,                                                     -- lab_test_ordg_prov_ssn
    NULL,                                                     -- lab_test_ordg_prov_nucc_taxnmy_cd
    NULL,                                                     -- lab_test_ordg_prov_alt_taxnmy_id
    NULL,                                                     -- lab_test_ordg_prov_alt_taxnmy_id_qual
    NULL,                                                     -- lab_test_ordg_prov_mdcr_speclty_cd
    NULL,                                                     -- lab_test_ordg_prov_alt_speclty_id
    NULL,                                                     -- lab_test_ordg_prov_alt_speclty_id_qual
    NULL,                                                     -- lab_test_ordg_prov_fclty_nm
    NULL,                                                     -- lab_test_ordg_prov_frst_nm
    NULL,                                                     -- lab_test_ordg_prov_last_nm
    NULL,                                                     -- lab_test_ordg_prov_addr_1_txt
    NULL,                                                     -- lab_test_ordg_prov_addr_2_txt
    NULL,                                                     -- lab_test_ordg_prov_state_cd
    NULL,                                                     -- lab_test_ordg_prov_zip_cd
    NULL,                                                     -- lab_test_exectg_fclty_npi
    l.practice_id,                                            -- lab_test_exectg_fclty_vdr_id
    CASE WHEN l.practice_id IS NOT NULL
    THEN 'VENDOR'
    END,                                                      -- lab_test_exectg_fclty_vdr_id_qual
    NULL,                                                     -- lab_test_exectg_fclty_alt_id
    NULL,                                                     -- lab_test_exectg_fclty_alt_id_qual
    NULL,                                                     -- lab_test_exectg_fclty_tax_id
    NULL,                                                     -- lab_test_exectg_fclty_dea_id
    NULL,                                                     -- lab_test_exectg_fclty_state_lic_id
    NULL,                                                     -- lab_test_exectg_fclty_comrcl_id
    NULL,                                                     -- lab_test_exectg_fclty_nucc_taxnmy_cd
    NULL,                                                     -- lab_test_exectg_fclty_alt_taxnmy_id
    NULL,                                                     -- lab_test_exectg_fclty_alt_taxnmy_id_qual
    NULL,                                                     -- lab_test_exectg_fclty_mdcr_speclty_cd
    NULL,                                                     -- lab_test_exectg_fclty_alt_speclty_id
    NULL,                                                     -- lab_test_exectg_fclty_alt_speclty_id_qual
    NULL,                                                     -- lab_test_exectg_fclty_fclty_nm
    NULL,                                                     -- lab_test_exectg_fclty_addr_1_txt
    NULL,                                                     -- lab_test_exectg_fclty_addr_2_txt
    NULL,                                                     -- lab_test_exectg_fclty_state_cd
    NULL,                                                     -- lab_test_exectg_fclty_zip_cd
    NULL,                                                     -- lab_test_specmn_typ_cd
    NULL,                                                     -- lab_test_fstg_stat_flg
    NULL,                                                     -- lab_test_panel_nm
    UPPER(l.test_name_specific),                              -- lab_test_nm
    NULL,                                                     -- lab_test_desc
    NULL,                                                     -- lab_test_loinc_cd
    NULL,                                                     -- lab_test_snomed_cd
    NULL,                                                     -- lab_test_vdr_cd
    NULL,                                                     -- lab_test_vdr_cd_qual
    NULL,                                                     -- lab_test_alt_cd
    NULL,                                                     -- lab_test_alt_cd_qual
    NULL,                                                     -- lab_result_nm
    NULL,                                                     -- lab_result_desc
    CASE
    WHEN CAST(l.test_value_string AS DECIMAL) IS NOT NULL
    THEN l.test_value_string
    END,                                                      -- lab_result_msrmt
    l.unit_desc,                                              -- lab_result_uom
    NULL,                                                     -- lab_result_qual
    NULL,                                                     -- lab_result_abnorm_flg
    l.min_norm,                                               -- lab_result_norm_min_msrmt
    l.max_norm,                                               -- lab_result_norm_max_msrmt
    NULL,                                                     -- lab_test_diag_cd
    NULL,                                                     -- lab_test_diag_cd_qual
    NULL,                                                     -- data_captr_dt
    NULL,                                                     -- rec_stat_cd
    'lab'                                                     -- prmy_src_tbl_nm
FROM lab_transactions l
    LEFT JOIN demographics_transactions_dedup d ON l.patient_id = d.patient_id
    LEFT JOIN matching_payload mp ON d.hvJoinKey = mp.hvJoinKey
WHERE l.import_source_id IS NOT NULL
