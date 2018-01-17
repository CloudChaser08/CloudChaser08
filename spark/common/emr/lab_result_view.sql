DROP VIEW IF EXISTS dw.hvm_emr_lab_result;
CREATE VIEW dw.hvm_emr_lab_result (
        row_id,
        hv_lab_result_id,
        crt_dt,
        mdl_vrsn_num,
        data_set_nm,
        src_vrsn_id,
        hvm_vdr_id,
        hvm_vdr_feed_id,
        vdr_org_id,
        vdr_lab_test_id,
        vdr_lab_test_id_qual,
        vdr_lab_result_id,
        vdr_lab_result_id_qual,
        hvid,
        ptnt_birth_yr,
        ptnt_age_num,
        ptnt_lvg_flg,
        ptnt_dth_dt,
        ptnt_gender_cd,
        ptnt_state_cd,
        ptnt_zip3_cd,
        hv_enc_id,
        enc_dt,
        hv_lab_ord_id,
        lab_test_smpl_collctn_dt,
        lab_test_schedd_dt,
        lab_test_execd_dt,
        lab_result_dt,
        lab_test_ordg_prov_taxnmy_id,
        lab_test_ordg_prov_speclty_id,
        lab_test_ordg_prov_state_cd,
        lab_test_ordg_prov_zip_cd,
        lab_test_exectg_fclty_taxnmy_id,
        lab_test_exectg_fclty_speclty_id,
        lab_test_exectg_fclty_state_cd,
        lab_test_exectg_fclty_zip_cd,
        lab_test_specmn_typ_cd,
        lab_test_fstg_stat_flg,
        lab_test_panel_nm,
        lab_test_nm,
        lab_test_desc,
        lab_test_loinc_cd,
        lab_test_snomed_cd,
        lab_test_vdr_cd,
        lab_test_vdr_cd_qual,
        lab_test_alt_cd,
        lab_test_alt_cd_qual,
        lab_result_nm,
        lab_result_desc,
        lab_result_msrmt,
        lab_result_uom,
        lab_result_qual,
        lab_result_abnorm_flg,
        lab_result_norm_min_msrmt,
        lab_result_norm_max_msrmt,
        lab_test_diag_cd,
        lab_test_diag_cd_qual,
        data_captr_dt,
        rec_stat_cd,
        part_hvm_vdr_feed_id,
        part_mth
        ) AS SELECT
    row_id,
    hv_lab_result_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    src_vrsn_id,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_lab_test_id,
    vdr_lab_test_id_qual,
    vdr_lab_result_id,
    vdr_lab_result_id_qual,
    hvid,
    ptnt_birth_yr,
    ptnt_age_num,
    ptnt_lvg_flg,
    ptnt_dth_dt,
    ptnt_gender_cd,
    ptnt_state_cd,
    ptnt_zip3_cd,
    hv_enc_id,
    enc_dt,
    hv_lab_ord_id,
    lab_test_smpl_collctn_dt,
    lab_test_schedd_dt,
    lab_test_execd_dt,
    lab_result_dt,
    COALESCE(lab_test_ordg_prov_nucc_taxnmy_cd, lab_test_ordg_prov_alt_taxnmy_id),
    COALESCE(lab_test_ordg_prov_mdcr_speclty_cd, lab_test_ordg_prov_alt_speclty_id),
    lab_test_ordg_prov_state_cd,
    lab_test_ordg_prov_zip_cd,
    COALESCE(lab_test_exectg_fclty_nucc_taxnmy_cd, lab_test_exectg_fclty_alt_taxnmy_id),
    COALESCE(lab_test_exectg_fclty_mdcr_speclty_cd, lab_test_exectg_fclty_alt_speclty_id),
    lab_test_exectg_fclty_state_cd,
    lab_test_exectg_fclty_zip_cd,
    lab_test_specmn_typ_cd,
    lab_test_fstg_stat_flg,
    lab_test_panel_nm,
    lab_test_nm,
    lab_test_desc,
    lab_test_loinc_cd,
    lab_test_snomed_cd,
    lab_test_vdr_cd,
    lab_test_vdr_cd_qual,
    lab_test_alt_cd,
    lab_test_alt_cd_qual,
    lab_result_nm,
    lab_result_desc,
    lab_result_msrmt,
    lab_result_uom,
    lab_result_qual,
    lab_result_abnorm_flg,
    lab_result_norm_min_msrmt,
    lab_result_norm_max_msrmt,
    lab_test_diag_cd,
    lab_test_diag_cd_qual,
    data_captr_dt,
    rec_stat_cd,
    part_hvm_vdr_feed_id,
    part_mth
FROM dw.emr_lab_result
    ;
