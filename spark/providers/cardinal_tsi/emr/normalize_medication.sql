INSERT INTO medication_common_model
SELECT
    NULL,                                         -- row_id
    CONCAT('31_', med.source, '_', med.uniq_id),  -- hv_medctn_id
    NULL,                                         -- crt_dt
    '02',                                         -- mdl_vrsn_num
    NULL,                                         -- data_set_nm
    NULL,                                         -- src_vrsn_id
    NULL,                                         -- hvm_vdr_id
    NULL,                                         -- hvm_vdr_feed_id
    med.source,                                   -- vdr_org_id
    med.uniq_id,                                  -- vdr_medctn_ord_id
    NULL,                                         -- vdr_medctn_ord_id_qual
    NULL,                                         -- vdr_medctn_admin_id
    NULL,                                         -- vdr_medctn_admin_id_qual
    mp.hvid,                                      -- hvid
    mp.yearOfBirth,                               -- ptnt_birth_yr
    mp.age,                                       -- ptnt_age_num
    NULL,                                         -- ptnt_lvg_flg
    NULL,                                         -- ptnt_dth_dt
    CASE
    WHEN mp.gender NOT IN ('M', 'F')
    THEN 'U' ELSE mp.gender
    END,                                          -- ptnt_gender_cd
    UPPER(mp.state),                              -- ptnt_state_cd
    mp.threeDigitZip,                             -- ptnt_zip3_cd
    NULL,                                         -- hv_enc_id
    NULL,                                         -- enc_dt
    NULL,                                         -- medctn_ord_dt
    NULL,                                         -- medctn_admin_dt
    NULL,                                         -- medctn_rndrg_fclty_npi
    NULL,                                         -- medctn_rndrg_fclty_vdr_id
    NULL,                                         -- medctn_rndrg_fclty_vdr_id_qual
    NULL,                                         -- medctn_rndrg_fclty_alt_id
    NULL,                                         -- medctn_rndrg_fclty_alt_id_qual
    NULL,                                         -- medctn_rndrg_fclty_tax_id
    NULL,                                         -- medctn_rndrg_fclty_dea_id
    NULL,                                         -- medctn_rndrg_fclty_state_lic_id
    NULL,                                         -- medctn_rndrg_fclty_comrcl_id
    NULL,                                         -- medctn_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                         -- medctn_rndrg_fclty_alt_taxnmy_id
    NULL,                                         -- medctn_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                         -- medctn_rndrg_fclty_mdcr_speclty_cd
    NULL,                                         -- medctn_rndrg_fclty_alt_speclty_id
    NULL,                                         -- medctn_rndrg_fclty_alt_speclty_id_qual
    NULL,                                         -- medctn_rndrg_fclty_fclty_nm
    NULL,                                         -- medctn_rndrg_fclty_addr_1_txt
    NULL,                                         -- medctn_rndrg_fclty_addr_2_txt
    NULL,                                         -- medctn_rndrg_fclty_state_cd
    NULL,                                         -- medctn_rndrg_fclty_zip_cd
    NULL,                                         -- medctn_ordg_prov_npi
    NULL,                                         -- medctn_ordg_prov_vdr_id
    NULL,                                         -- medctn_ordg_prov_vdr_id_qual
    NULL,                                         -- medctn_ordg_prov_alt_id
    NULL,                                         -- medctn_ordg_prov_alt_id_qual
    NULL,                                         -- medctn_ordg_prov_tax_id
    NULL,                                         -- medctn_ordg_prov_dea_id
    NULL,                                         -- medctn_ordg_prov_state_lic_id
    NULL,                                         -- medctn_ordg_prov_comrcl_id
    NULL,                                         -- medctn_ordg_prov_upin
    NULL,                                         -- medctn_ordg_prov_ssn
    NULL,                                         -- medctn_ordg_prov_nucc_taxnmy_cd
    NULL,                                         -- medctn_ordg_prov_alt_taxnmy_id
    NULL,                                         -- medctn_ordg_prov_alt_taxnmy_id_qual
    NULL,                                         -- medctn_ordg_prov_mdcr_speclty_cd
    NULL,                                         -- medctn_ordg_prov_alt_speclty_id
    NULL,                                         -- medctn_ordg_prov_alt_speclty_id_qual
    NULL,                                         -- medctn_ordg_prov_frst_nm
    NULL,                                         -- medctn_ordg_prov_last_nm
    NULL,                                         -- medctn_ordg_prov_addr_1_txt
    NULL,                                         -- medctn_ordg_prov_addr_2_txt
    NULL,                                         -- medctn_ordg_prov_state_cd
    NULL,                                         -- medctn_ordg_prov_zip_cd
    NULL,                                         -- medctn_adminrg_fclty_npi
    NULL,                                         -- medctn_adminrg_fclty_vdr_id
    NULL,                                         -- medctn_adminrg_fclty_vdr_id_qual
    NULL,                                         -- medctn_adminrg_fclty_alt_id
    NULL,                                         -- medctn_adminrg_fclty_alt_id_qual
    NULL,                                         -- medctn_adminrg_fclty_tax_id
    NULL,                                         -- medctn_adminrg_fclty_dea_id
    NULL,                                         -- medctn_adminrg_fclty_state_lic_id
    NULL,                                         -- medctn_adminrg_fclty_comrcl_id
    NULL,                                         -- medctn_adminrg_fclty_nucc_taxnmy_cd
    NULL,                                         -- medctn_adminrg_fclty_alt_taxnmy_id
    NULL,                                         -- medctn_adminrg_fclty_alt_taxnmy_id_qual
    NULL,                                         -- medctn_adminrg_fclty_mdcr_speclty_cd
    NULL,                                         -- medctn_adminrg_fclty_alt_speclty_id
    NULL,                                         -- medctn_adminrg_fclty_alt_speclty_id_qual
    NULL,                                         -- medctn_adminrg_fclty_fclty_nm
    NULL,                                         -- medctn_adminrg_fclty_addr_1_txt
    NULL,                                         -- medctn_adminrg_fclty_addr_2_txt
    NULL,                                         -- medctn_adminrg_fclty_state_cd
    NULL,                                         -- medctn_adminrg_fclty_zip_cd
    NULL,                                         -- rx_num
    EXTRACT_DATE(
        med.start_date,
        '%Y%m%d',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                        -- medctn_start_dt
    EXTRACT_DATE(
        med.date_stopped,
        '%Y%m%d',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                        -- medctn_end_dt
    NULL,                                         -- medctn_diag_cd
    NULL,                                         -- medctn_diag_cd_qual
    med.ndc_id,                                   -- medctn_ndc
    NULL,                                         -- medctn_lblr_cd
    NULL,                                         -- medctn_drug_and_strth_cd
    NULL,                                         -- medctn_pkg_cd
    NULL,                                         -- medctn_hicl_thrptc_cls_cd
    NULL,                                         -- medctn_hicl_cd
    NULL,                                         -- medctn_gcn_cd
    NULL,                                         -- medctn_rxnorm_cd
    NULL,                                         -- medctn_snomed_cd
    med.generic_ok_ind,                           -- medctn_genc_ok_flg
    NULL,                                         -- medctn_brd_nm
    NULL,                                         -- medctn_genc_nm
    NULL,                                         -- medctn_rx_flg
    med.rx_quantity,                              -- medctn_rx_qty
    NULL,                                         -- medctn_dly_qty
    NULL,                                         -- medctn_dispd_qty
    NULL,                                         -- medctn_days_supply_qty
    NULL,                                         -- medctn_admin_unt_qty
    NULL,                                         -- medctn_admin_freq_qty
    NULL,                                         -- medctn_admin_sched_cd
    NULL,                                         -- medctn_admin_sched_qty
    NULL,                                         -- medctn_admin_sig_cd
    med.sig_desc,                                 -- medctn_admin_sig_txt
    med.rx_units,                                 -- medctn_admin_form_nm
    NULL,                                         -- medctn_specl_pkgg_cd
    NULL,                                         -- medctn_strth_txt
    NULL,                                         -- medctn_strth_txt_qual
    NULL,                                         -- medctn_dose_txt
    NULL,                                         -- medctn_dose_txt_qual
    NULL,                                         -- medctn_admin_rte_txt
    med.rx_refills,                               -- medctn_orig_rfll_qty
    NULL,                                         -- medctn_fll_num
    NULL,                                         -- medctn_remng_rfll_qty
    EXTRACT_DATE(
        med.date_last_refilled,
        '%Y%m%d',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
        ),                                        -- medctn_last_rfll_dt
    NULL,                                         -- medctn_smpl_flg
    NULL,                                         -- medctn_elect_rx_flg
    NULL,                                         -- medctn_verfd_flg
    NULL,                                         -- medctn_prod_svc_id
    NULL,                                         -- medctn_prod_svc_id_qual
    NULL,                                         -- data_captr_dt
    NULL,                                         -- rec_stat_cd
    'patient_medication'                          -- prmy_src_tbl_nm
FROM transactions_medication med
    LEFT JOIN matching_payload mp ON UPPER(med.person_id) = UPPER(mp.personid)
