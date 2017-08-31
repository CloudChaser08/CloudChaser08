INSERT INTO medication_common_model
SELECT 
    NULL,                                                       -- rec_id
    CONCAT('40_', disp.id),                                     -- hv_medctn_id
    NULL,                                                       -- crt_dt
    '03',                                                       -- mdl_vrsn_num
    NULL,                                                       -- data_set_nm
    NULL,                                                       -- src_vrsn_id
    NULL,                                                       -- hvm_vdr_id
    NULL,                                                       -- hvm_vdr_feed_id
    NULL,                                                       -- vdr_org_id
    NULL,                                                       -- vdr_medctn_ord_id
    NULL,                                                       -- vdr_medctn_ord_id_qual
    disp.id,                                                    -- vdr_medctn_admin_id
    NULL,                                                       -- vdr_medctn_admin_id_qual
    mp.hvid,                                                    -- hvid
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
    NULL,                                                       -- medctn_ord_dt
    EXTRACT_DATE(
        disp.admin_date,
        '%Y-%m-%d %H:%M:%S.%f'
        ),                                                      -- medctn_admin_dt
    NULL,                                                       -- medctn_rndrg_fclty_npi
    disp.practice_id,                                           -- medctn_rndrg_fclty_vdr_id
    NULL,                                                       -- medctn_rndrg_fclty_vdr_id_qual
    NULL,                                                       -- medctn_rndrg_fclty_alt_id
    NULL,                                                       -- medctn_rndrg_fclty_alt_id_qual
    NULL,                                                       -- medctn_rndrg_fclty_tax_id
    NULL,                                                       -- medctn_rndrg_fclty_dea_id
    NULL,                                                       -- medctn_rndrg_fclty_state_lic_id
    NULL,                                                       -- medctn_rndrg_fclty_comrcl_id
    NULL,                                                       -- medctn_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                                       -- medctn_rndrg_fclty_alt_taxnmy_id
    NULL,                                                       -- medctn_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                                       -- medctn_rndrg_fclty_mdcr_speclty_cd
    NULL,                                                       -- medctn_rndrg_fclty_alt_speclty_id
    NULL,                                                       -- medctn_rndrg_fclty_alt_speclty_id_qual
    NULL,                                                       -- medctn_rndrg_fclty_fclty_nm
    NULL,                                                       -- medctn_rndrg_fclty_addr_1_txt
    NULL,                                                       -- medctn_rndrg_fclty_addr_2_txt
    NULL,                                                       -- medctn_rndrg_fclty_state_cd
    NULL,                                                       -- medctn_rndrg_fclty_zip_cd
    disp.npi,                                                   -- medctn_ordg_prov_npi
    NULL,                                                       -- medctn_ordg_prov_vdr_id
    NULL,                                                       -- medctn_ordg_prov_vdr_id_qual
    NULL,                                                       -- medctn_ordg_prov_alt_id
    NULL,                                                       -- medctn_ordg_prov_alt_id_qual
    NULL,                                                       -- medctn_ordg_prov_tax_id
    NULL,                                                       -- medctn_ordg_prov_dea_id
    NULL,                                                       -- medctn_ordg_prov_state_lic_id
    NULL,                                                       -- medctn_ordg_prov_comrcl_id
    NULL,                                                       -- medctn_ordg_prov_upin
    NULL,                                                       -- medctn_ordg_prov_ssn
    NULL,                                                       -- medctn_ordg_prov_nucc_taxnmy_cd
    NULL,                                                       -- medctn_ordg_prov_alt_taxnmy_id
    NULL,                                                       -- medctn_ordg_prov_alt_taxnmy_id_qual
    NULL,                                                       -- medctn_ordg_prov_mdcr_speclty_cd
    NULL,                                                       -- medctn_ordg_prov_alt_speclty_id
    NULL,                                                       -- medctn_ordg_prov_alt_speclty_id_qual
    disp.ordered_by_name,                                       -- medctn_ordg_prov_frst_nm
    disp.ordered_by_name,                                       -- medctn_ordg_prov_last_nm
    NULL,                                                       -- medctn_ordg_prov_addr_1_txt
    NULL,                                                       -- medctn_ordg_prov_addr_2_txt
    NULL,                                                       -- medctn_ordg_prov_state_cd
    NULL,                                                       -- medctn_ordg_prov_zip_cd
    NULL,                                                       -- medctn_adminrg_fclty_npi
    disp.dest_pharm_id,                                         -- medctn_adminrg_fclty_vdr_id
    NULL,                                                       -- medctn_adminrg_fclty_vdr_id_qual
    NULL,                                                       -- medctn_adminrg_fclty_alt_id
    NULL,                                                       -- medctn_adminrg_fclty_alt_id_qual
    NULL,                                                       -- medctn_adminrg_fclty_tax_id
    NULL,                                                       -- medctn_adminrg_fclty_dea_id
    NULL,                                                       -- medctn_adminrg_fclty_state_lic_id
    NULL,                                                       -- medctn_adminrg_fclty_comrcl_id
    NULL,                                                       -- medctn_adminrg_fclty_nucc_taxnmy_cd
    NULL,                                                       -- medctn_adminrg_fclty_alt_taxnmy_id
    NULL,                                                       -- medctn_adminrg_fclty_alt_taxnmy_id_qual
    NULL,                                                       -- medctn_adminrg_fclty_mdcr_speclty_cd
    NULL,                                                       -- medctn_adminrg_fclty_alt_speclty_id
    NULL,                                                       -- medctn_adminrg_fclty_alt_speclty_id_qual
    NULL,                                                       -- medctn_adminrg_fclty_fclty_nm
    NULL,                                                       -- medctn_adminrg_fclty_addr_1_txt
    NULL,                                                       -- medctn_adminrg_fclty_addr_2_txt
    NULL,                                                       -- medctn_adminrg_fclty_state_cd
    NULL,                                                       -- medctn_adminrg_fclty_zip_cd
    disp.prescription_number,                                   -- rx_num
    NULL,                                                       -- medctn_start_dt
    EXTRACT_DATE(
        disp.discontinue_date,
        '%Y-%m-%d %H:%M:%S.%f'
        ),                                                      -- medctn_end_dt
    COALESCE(disp.icd_ten, disp.icd_nine),                      -- medctn_diag_cd
    CASE
    WHEN disp.icd_ten IS NOT NULL THEN '02' 
    WHEN disp.icd_nine IS NOT NULL THEN '01'
    END,                                                        -- medctn_diag_cd_qual
    disp.ndc_written,                                           -- medctn_ndc
    disp.ndc_labeler_code,                                      -- medctn_lblr_cd
    disp.ndc_drug_and_strength,                                 -- medctn_drug_and_strth_cd
    disp.ndc_package_size,                                      -- medctn_pkg_cd
    NULL,                                                       -- medctn_hicl_thrptc_cls_cd
    NULL,                                                       -- medctn_hicl_cd
    NULL,                                                       -- medctn_gcn_cd
    NULL,                                                       -- medctn_rxnorm_cd
    NULL,                                                       -- medctn_snomed_cd
    NULL,                                                       -- medctn_genc_ok_flg
    disp.brand_name,                                            -- medctn_brd_nm
    disp.generic_name,                                          -- medctn_genc_nm
    NULL,                                                       -- medctn_rx_flg
    NULL,                                                       -- medctn_rx_qty
    disp.quantity_per_day,                                      -- medctn_dly_qty
    CASE
    WHEN CAST(disp.qty AS INT) = 0 THEN NULL
    ELSE disp.qty
    END,                                                        -- medctn_dispd_qty
    disp.days_supply,                                           -- medctn_days_supply_qty
    CASE
    WHEN CAST(disp.num_doses AS INT) = 0 THEN NULL
    ELSE disp.num_doses
    END,                                                        -- medctn_admin_unt_qty
    NULL,                                                       -- medctn_admin_freq_qty
    NULL,                                                       -- medctn_admin_sched_cd
    NULL,                                                       -- medctn_admin_sched_qty
    NULL,                                                       -- medctn_admin_sig_cd
    UPPER(disp.instructions),                                   -- medctn_admin_sig_txt
    UPPER(disp.dosage_form),                                    -- medctn_admin_form_nm
    NULL,                                                       -- medctn_specl_pkgg_cd
    UPPER(disp.strength),                                       -- medctn_strth_txt
    UPPER(disp.strength_unit),                                  -- medctn_strth_txt_qual
    NULL,                                                       -- medctn_dose_txt
    NULL,                                                       -- medctn_dose_txt_qual
    UPPER(disp.admn_route),                                     -- medctn_admin_rte_txt
    NULL,                                                       -- medctn_orig_rfll_qty
    NULL,                                                       -- medctn_fll_num
    disp.refills,                                               -- medctn_remng_rfll_qty
    NULL,                                                       -- medctn_last_rfll_dt
    NULL,                                                       -- medctn_smpl_flg
    NULL,                                                       -- medctn_elect_rx_flg
    NULL,                                                       -- medctn_verfd_flg
    NULL,                                                       -- medctn_prod_svc_id
    NULL,                                                       -- medctn_prod_svc_id_qual
    NULL,                                                       -- data_captr_dt
    NULL,                                                       -- rec_stat_cd
    'order_dispense'                                            -- prmy_src_tbl_nm
FROM dispense_transactions disp
    LEFT JOIN demographics_transactions_dedup dem ON disp.patient_id = dem.patient_id
    LEFT JOIN matching_payload mp ON dem.hvJoinKey = mp.hvJoinKey
WHERE disp.import_source_id IS NOT NULL
