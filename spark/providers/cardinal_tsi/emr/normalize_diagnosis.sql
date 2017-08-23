INSERT INTO diagnosis_common_model
SELECT 
    NULL,                                     -- row_id
    CONCAT('31_', d.source, '_', d.uniq_id),  -- hv_diag_id
    NULL,                                     -- crt_dt
    '03',                                     -- mdl_vrsn_num
    NULL,                                     -- data_set_nm
    NULL,                                     -- src_vrsn_id
    NULL,                                     -- hvm_vdr_id
    NULL,                                     -- hvm_vdr_feed_id
    d.source,                                 -- vdr_org_id
    d.uniq_id,                                -- vdr_diag_id
    NULL,                                     -- vdr_diag_id_qual
    mp.hvid,                                  -- hvid
    mp.yearOfBirth,                           -- ptnt_birth_yr
    mp.age,                                   -- ptnt_age_num
    NULL,                                     -- ptnt_lvg_flg
    NULL,                                     -- ptnt_dth_dt
    CASE
    WHEN mp.gender NOT IN ('M', 'F')
    THEN 'U' ELSE mp.gender
    END,                                      -- ptnt_gender_cd
    UPPER(mp.state),                          -- ptnt_state_cd
    mp.threeDigitZip,                         -- ptnt_zip3_cd
    NULL,                                     -- hv_enc_id
    EXTRACT_DATE(
        d.enc_timestamp,
        '%Y-%m-%d %H:%M:%S',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                    -- enc_dt
    NULL,                                     -- diag_dt
    NULL,                                     -- diag_rndrg_fclty_npi
    NULL,                                     -- diag_rndrg_fclty_vdr_id
    NULL,                                     -- diag_rndrg_fclty_vdr_id_qual
    NULL,                                     -- diag_rndrg_fclty_alt_id
    NULL,                                     -- diag_rndrg_fclty_alt_id_qual
    NULL,                                     -- diag_rndrg_fclty_tax_id
    NULL,                                     -- diag_rndrg_fclty_dea_id
    NULL,                                     -- diag_rndrg_fclty_state_lic_id
    NULL,                                     -- diag_rndrg_fclty_comrcl_id
    NULL,                                     -- diag_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                     -- diag_rndrg_fclty_alt_taxnmy_id
    NULL,                                     -- diag_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                     -- diag_rndrg_fclty_mdcr_speclty_cd
    NULL,                                     -- diag_rndrg_fclty_alt_speclty_id
    NULL,                                     -- diag_rndrg_fclty_alt_speclty_id_qual
    NULL,                                     -- diag_rndrg_fclty_fclty_nm
    NULL,                                     -- diag_rndrg_fclty_addr_1_txt
    NULL,                                     -- diag_rndrg_fclty_addr_2_txt
    NULL,                                     -- diag_rndrg_fclty_state_cd
    NULL,                                     -- diag_rndrg_fclty_zip_cd
    NULL,                                     -- diag_rndrg_prov_npi
    NULL,                                     -- diag_rndrg_prov_vdr_id
    NULL,                                     -- diag_rndrg_prov_vdr_id_qual
    NULL,                                     -- diag_rndrg_prov_alt_id
    NULL,                                     -- diag_rndrg_prov_alt_id_qual
    NULL,                                     -- diag_rndrg_prov_tax_id
    NULL,                                     -- diag_rndrg_prov_dea_id
    NULL,                                     -- diag_rndrg_prov_state_lic_id
    NULL,                                     -- diag_rndrg_prov_comrcl_id
    NULL,                                     -- diag_rndrg_prov_upin
    NULL,                                     -- diag_rndrg_prov_ssn
    NULL,                                     -- diag_rndrg_prov_nucc_taxnmy_cd
    NULL,                                     -- diag_rndrg_prov_alt_taxnmy_id
    NULL,                                     -- diag_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                     -- diag_rndrg_prov_mdcr_speclty_cd
    NULL,                                     -- diag_rndrg_prov_alt_speclty_id
    NULL,                                     -- diag_rndrg_prov_alt_speclty_id_qual
    NULL,                                     -- diag_rndrg_prov_frst_nm
    NULL,                                     -- diag_rndrg_prov_last_nm
    NULL,                                     -- diag_rndrg_prov_addr_1_txt
    NULL,                                     -- diag_rndrg_prov_addr_2_txt
    NULL,                                     -- diag_rndrg_prov_state_cd
    NULL,                                     -- diag_rndrg_prov_zip_cd
    NULL,                                     -- diag_onset_dt
    NULL,                                     -- diag_resltn_dt
    d.diagnosis_code_id,                      -- diag_cd
    NULL,                                     -- diag_cd_qual
    NULL,                                     -- diag_alt_cd
    NULL,                                     -- diag_alt_cd_qual
    NULL,                                     -- diag_nm
    d.description,                            -- diag_desc
    NULL,                                     -- diag_prty_cd
    NULL,                                     -- diag_prty_cd_qual
    NULL,                                     -- diag_svty_cd
    NULL,                                     -- diag_svty_cd_qual
    NULL,                                     -- diag_resltn_cd
    NULL,                                     -- diag_resltn_cd_qual
    NULL,                                     -- diag_resltn_nm
    NULL,                                     -- diag_resltn_desc
    NULL,                                     -- diag_stat_cd
    NULL,                                     -- diag_stat_cd_qual
    NULL,                                     -- diag_stat_nm
    NULL,                                     -- diag_stat_desc
    NULL,                                     -- diag_snomed_cd
    NULL,                                     -- diag_meth_cd
    NULL,                                     -- diag_meth_cd_qual
    NULL,                                     -- diag_meth_nm
    NULL,                                     -- diag_meth_desc
    NULL,                                     -- data_captr_dt
    NULL,                                     -- rec_stat_cd
    'patient_diagnosis'                       -- prmy_src_tbl_nm
FROM transactions_diagnosis d
    LEFT JOIN matching_payload mp ON UPPER(d.person_id) = UPPER(mp.personid)
    ;
