INSERT INTO lab_order_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_lab_ord_id
    NULL,                                   -- crt_dt
    '3',                                    -- mdl_vrsn_num
    NULL,                                   -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    NULL,                                   -- vdr_org_id
    NULL,                                   -- vdr_lab_ord_id
    NULL,                                   -- vdr_lab_ord_id_qual
    pay.hvid,                               -- hvid
    pay.yearOfBirth,                        -- ptnt_birth_yr
    NULL,                                   -- ptnt_age_num
    NULL,                                   -- ptnt_lvg_flg
    NULL,                                   -- ptnt_dth_dt
    CASE
      WHEN UPPER(COALESCE(pay.gender, dem.gender)) IN ('F', 'M')
      THEN UPPER(COALESCE(pay.gender, dem.gender))
      ELSE 'U'
    END,                                    -- ptnt_gender_cd
    UPPER(COALESCE(pay.state, dem.state)),  -- ptnt_state_cd
    pay.threeDigitZip,                      -- ptnt_zip3_cd
    NULL,                                   -- hv_enc_id
    NULL,                                   -- enc_dt
    CASE
      WHEN LENGTH(lab.date) = 8 AND LOCATE('/', lab.date) = 0
      THEN EXTRACT_DATE(lab.date, '%Y%m%d')
      WHEN LENGTH(lab.date) = 10 AND LOCATE('/', lab.date) <> 0
      THEN EXTRACT_DATE(lab.date, '%m/%d/%Y')
    END,                                    -- lab_ord_dt
    NULL,                                   -- lab_ord_test_schedd_dt
    NULL,                                   -- lab_ord_smpl_collctn_dt
    NULL,                                   -- lab_ord_ordg_prov_npi
    NULL,                                   -- lab_ord_ordg_prov_vdr_id
    NULL,                                   -- lab_ord_ordg_prov_vdr_id_qual
    NULL,                                   -- lab_ord_ordg_prov_alt_id
    NULL,                                   -- lab_ord_ordg_prov_alt_id_qual
    NULL,                                   -- lab_ord_ordg_prov_tax_id
    NULL,                                   -- lab_ord_ordg_prov_dea_id
    NULL,                                   -- lab_ord_ordg_prov_state_lic_id
    NULL,                                   -- lab_ord_ordg_prov_comrcl_id
    NULL,                                   -- lab_ord_ordg_prov_upin
    NULL,                                   -- lab_ord_ordg_prov_ssn
    NULL,                                   -- lab_ord_ordg_prov_nucc_taxnmy_cd
    NULL,                                   -- lab_ord_ordg_prov_alt_taxnmy_id
    NULL,                                   -- lab_ord_ordg_prov_alt_taxnmy_id_qual
    NULL,                                   -- lab_ord_ordg_prov_mdcr_speclty_cd
    NULL,                                   -- lab_ord_ordg_prov_alt_speclty_id
    NULL,                                   -- lab_ord_ordg_prov_alt_speclty_id_qual
    NULL,                                   -- lab_ord_ordg_prov_fclty_nm
    NULL,                                   -- lab_ord_ordg_prov_frst_nm
    NULL,                                   -- lab_ord_ordg_prov_last_nm
    NULL,                                   -- lab_ord_ordg_prov_addr_1_txt
    NULL,                                   -- lab_ord_ordg_prov_addr_2_txt
    NULL,                                   -- lab_ord_ordg_prov_state_cd
    NULL,                                   -- lab_ord_ordg_prov_zip_cd
    lab.loinc,                              -- lab_ord_loinc_cd
    NULL,                                   -- lab_ord_snomed_cd
    NULL,                                   -- lab_ord_alt_cd
    NULL,                                   -- lab_ord_alt_cd_qual
    NULL,                                   -- lab_ord_test_nm
    NULL,                                   -- lab_ord_panel_nm
    NULL,                                   -- lab_ord_diag_cd
    NULL,                                   -- lab_ord_diag_cd_qual
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    NULL                                    -- prmy_src_tbl_nm
FROM loinc_transactions lab
    LEFT JOIN demographics_transactions dem ON lab.id = dem.demographicid
    LEFT JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
