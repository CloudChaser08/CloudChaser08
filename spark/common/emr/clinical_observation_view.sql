DROP VIEW IF EXISTS dw.hvm_emr_clin_obsn;
CREATE VIEW dw.hvm_emr_clin_obsn (
        row_id,
        hv_clin_obsn_id,
        crt_dt,
        mdl_vrsn_num,
        data_set_nm,
        src_vrsn_id,
        hvm_vdr_id,
        hvm_vdr_feed_id,
        vdr_org_id,
        vdr_clin_obsn_id,
        vdr_clin_obsn_id_qual,
        vdr_alt_clin_obsn_id,
        vdr_alt_clin_obsn_id_qual,
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
        clin_obsn_dt,
        clin_obsn_rndrg_fclty_taxnmy_id,
        clin_obsn_rndrg_fclty_speclty_id,
        clin_obsn_rndrg_fclty_state_cd,
        clin_obsn_rndrg_fclty_zip_cd,
        clin_obsn_rndrg_prov_taxnmy_id,
        clin_obsn_rndrg_prov_speclty_id,
        clin_obsn_rndrg_prov_state_cd,
        clin_obsn_rndrg_prov_zip_cd,
        clin_obsn_onset_dt,
        clin_obsn_resltn_dt,
        clin_obsn_data_ctgy_cd,
        clin_obsn_data_ctgy_cd_qual,
        clin_obsn_data_ctgy_nm,
        clin_obsn_data_ctgy_desc,
        clin_obsn_typ_cd,
        clin_obsn_typ_cd_qual,
        clin_obsn_typ_nm,
        clin_obsn_typ_desc,
        clin_obsn_substc_cd,
        clin_obsn_substc_cd_qual,
        clin_obsn_substc_nm,
        clin_obsn_substc_desc,
        clin_obsn_cd,
        clin_obsn_cd_qual,
        clin_obsn_nm,
        clin_obsn_desc,
        clin_obsn_ndc,
        clin_obsn_diag_cd,
        clin_obsn_diag_cd_qual,
        clin_obsn_diag_nm,
        clin_obsn_snomed_cd,
        clin_obsn_result_cd,
        clin_obsn_result_cd_qual,
        clin_obsn_result_nm,
        clin_obsn_result_desc,
        clin_obsn_msrmt,
        clin_obsn_uom,
        clin_obsn_qual,
        clin_obsn_abnorm_flg,
        clin_obsn_norm_min_msrmt,
        clin_obsn_norm_max_msrmt,
        clin_obsn_stat_cd,
        clin_obsn_stat_cd_qual,
        clin_obsn_stat_nm,
        clin_obsn_verfd_by_prov_flg,
        data_src_cd,
        data_captr_dt,
        rec_stat_cd,
        prmy_src_tbl_nm,
        part_hvm_vdr_feed_id,
        part_mth
        ) AS SELECT
    row_id,
    hv_clin_obsn_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    src_vrsn_id,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_clin_obsn_id,
    vdr_clin_obsn_id_qual,
    vdr_alt_clin_obsn_id,
    vdr_alt_clin_obsn_id_qual,
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
    clin_obsn_dt,
    COALESCE(clin_obsn_rndrg_fclty_nucc_taxnmy_cd, clin_obsn_rndrg_fclty_alt_taxnmy_id),
    COALESCE(clin_obsn_rndrg_fclty_mdcr_speclty_cd, clin_obsn_rndrg_fclty_alt_speclty_id),
    clin_obsn_rndrg_fclty_state_cd,
    clin_obsn_rndrg_fclty_zip_cd,
    COALESCE(clin_obsn_rndrg_prov_nucc_taxnmy_cd, clin_obsn_rndrg_prov_alt_taxnmy_id),
    COALESCE(clin_obsn_rndrg_prov_mdcr_speclty_cd, clin_obsn_rndrg_prov_alt_speclty_id),
    clin_obsn_rndrg_prov_state_cd,
    clin_obsn_rndrg_prov_zip_cd,
    clin_obsn_onset_dt,
    clin_obsn_resltn_dt,
    clin_obsn_data_ctgy_cd,
    clin_obsn_data_ctgy_cd_qual,
    clin_obsn_data_ctgy_nm,
    clin_obsn_data_ctgy_desc,
    clin_obsn_typ_cd,
    clin_obsn_typ_cd_qual,
    clin_obsn_typ_nm,
    clin_obsn_typ_desc,
    clin_obsn_substc_cd,
    clin_obsn_substc_cd_qual,
    clin_obsn_substc_nm,
    clin_obsn_substc_desc,
    clin_obsn_cd,
    clin_obsn_cd_qual,
    clin_obsn_nm,
    clin_obsn_desc,
    clin_obsn_ndc,
    clin_obsn_diag_cd,
    clin_obsn_diag_cd_qual,
    clin_obsn_diag_nm,
    clin_obsn_snomed_cd,
    clin_obsn_result_cd,
    clin_obsn_result_cd_qual,
    clin_obsn_result_nm,
    clin_obsn_result_desc,
    clin_obsn_msrmt,
    clin_obsn_uom,
    clin_obsn_qual,
    clin_obsn_abnorm_flg,
    clin_obsn_norm_min_msrmt,
    clin_obsn_norm_max_msrmt,
    clin_obsn_stat_cd,
    clin_obsn_stat_cd_qual,
    clin_obsn_stat_nm,
    clin_obsn_verfd_by_prov_flg,
    data_src_cd,
    data_captr_dt,
    rec_stat_cd,
    prmy_src_tbl_nm,
    part_hvm_vdr_feed_id,
    part_mth
FROM dw.emr_clin_obsn
    ;
