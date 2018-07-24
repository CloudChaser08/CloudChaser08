DROP VIEW IF EXISTS dw.hvm_emr_lab_ord;
CREATE VIEW dw.hvm_emr_lab_ord (
        row_id,
        hv_lab_ord_id,
        crt_dt,
        mdl_vrsn_num,
        data_set_nm,
        src_vrsn_id,
        hvm_vdr_id,
        hvm_vdr_feed_id,
        vdr_org_id,
        vdr_lab_ord_id,
        vdr_lab_ord_id_qual,
        vdr_alt_lab_ord_id,
        vdr_alt_lab_ord_id_qual,
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
        lab_ord_dt,
        lab_ord_test_schedd_dt,
        lab_ord_smpl_collctn_dt,
        lab_ord_ordg_prov_taxnmy_id,
        lab_ord_ordg_prov_speclty_id,
        lab_ord_ordg_prov_state_cd,
        lab_ord_ordg_prov_zip_cd,
        lab_ord_loinc_cd,
        lab_ord_snomed_cd,
        lab_ord_alt_cd,
        lab_ord_alt_cd_qual,
        lab_ord_test_nm,
        lab_ord_panel_nm,
        lab_ord_diag_cd,
        lab_ord_diag_cd_qual,
        lab_ord_stat_cd,
        lab_ord_stat_cd_qual,
        data_src_cd,
        data_captr_dt,
        rec_stat_cd,
        prmy_src_tbl_nm,
        part_hvm_vdr_feed_id,
        part_mth
        ) AS SELECT
    row_id,
    hv_lab_ord_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    src_vrsn_id,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_lab_ord_id,
    vdr_lab_ord_id_qual,
    vdr_alt_lab_ord_id,
    vdr_alt_lab_ord_id_qual,
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
    lab_ord_dt,
    lab_ord_test_schedd_dt,
    lab_ord_smpl_collctn_dt,
    COALESCE(lab_ord_ordg_prov_nucc_taxnmy_cd, lab_ord_ordg_prov_alt_taxnmy_id),
    COALESCE(lab_ord_ordg_prov_mdcr_speclty_cd, lab_ord_ordg_prov_alt_speclty_id),
    lab_ord_ordg_prov_state_cd,
    lab_ord_ordg_prov_zip_cd,
    lab_ord_loinc_cd,
    lab_ord_snomed_cd,
    lab_ord_alt_cd,
    lab_ord_alt_cd_qual,
    lab_ord_test_nm,
    lab_ord_panel_nm,
    lab_ord_diag_cd,
    lab_ord_diag_cd_qual,
    lab_ord_stat_cd,
    lab_ord_stat_cd_qual,
    data_src_cd,
    data_captr_dt,
    rec_stat_cd,
    prmy_src_tbl_nm,
    part_hvm_vdr_feed_id,
    part_mth
FROM dw.emr_lab_ord
    ;
