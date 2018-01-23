DROP VIEW IF EXISTS dw.hvm_emr_prov_ord;
CREATE VIEW dw.hvm_emr_prov_ord (
        row_id,
        hv_prov_ord_id,
        crt_dt,
        mdl_vrsn_num,
        data_set_nm,
        src_vrsn_id,
        hvm_vdr_id,
        hvm_vdr_feed_id,
        vdr_org_id,
        vdr_prov_ord_id,
        vdr_prov_ord_id_qual,
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
        prov_ord_dt,
        ordg_prov_taxnmy_id,
        ordg_prov_speclty_id,
        ordg_prov_state_cd,
        ordg_prov_zip_cd,
        prov_ord_start_dt,
        prov_ord_end_dt,
        prov_ord_ctgy_cd,
        prov_ord_ctgy_cd_qual,
        prov_ord_ctgy_nm,
        prov_ord_ctgy_desc,
        prov_ord_typ_cd,
        prov_ord_typ_cd_qual,
        prov_ord_typ_nm,
        prov_ord_typ_desc,
        prov_ord_cd,
        prov_ord_cd_qual,
        prov_ord_nm,
        prov_ord_desc,
        prov_ord_alt_cd,
        prov_ord_alt_cd_qual,
        prov_ord_alt_nm,
        prov_ord_alt_desc,
        prov_ord_diag_cd,
        prov_ord_diag_cd_qual,
        prov_ord_diag_nm,
        prov_ord_diag_desc,
        prov_ord_snomed_cd,
        prov_ord_vcx_cd,
        prov_ord_vcx_cd_qual,
        prov_ord_vcx_nm,
        prov_ord_vcx_desc,
        prov_ord_rsn_cd,
        prov_ord_rsn_cd_qual,
        prov_ord_rsn_nm,
        prov_ord_rsn_desc,
        prov_ord_stat_cd,
        prov_ord_stat_cd_qual,
        prov_ord_stat_nm,
        prov_ord_stat_desc,
        prov_ord_complt_flg,
        prov_ord_complt_dt,
        prov_ord_complt_rsn_cd,
        prov_ord_complt_rsn_cd_qual,
        prov_ord_complt_rsn_nm,
        prov_ord_complt_rsn_desc,
        prov_ord_cxld_rsn_cd,
        prov_ord_cxld_rsn_cd_qual,
        prov_ord_cxld_rsn_nm,
        prov_ord_cxld_rsn_desc,
        prov_ord_cxld_dt,
        prov_ord_result_cd,
        prov_ord_result_cd_qual,
        prov_ord_result_nm,
        prov_ord_result_desc,
        prov_ord_result_rcvd_dt,
        prov_ord_trtmt_typ_cd,
        prov_ord_trtmt_typ_cd_qual,
        prov_ord_trtmt_typ_nm,
        prov_ord_trtmt_typ_desc,
        prov_ord_rfrd_speclty_cd,
        prov_ord_rfrd_speclty_cd_qual,
        prov_ord_rfrd_speclty_nm,
        prov_ord_rfrd_speclty_desc,
        prov_ord_specl_instrs_cd,
        prov_ord_specl_instrs_cd_qual,
        prov_ord_specl_instrs_nm,
        prov_ord_specl_instrs_desc,
        prov_ord_edctn_flg,
        prov_ord_edctn_dt,
        prov_ord_edctn_cd,
        prov_ord_edctn_cd_qual,
        prov_ord_edctn_nm,
        prov_ord_edctn_desc,
        data_captr_dt,
        rec_stat_cd,
        part_hvm_vdr_feed_id,
        part_mth
        ) AS SELECT
    row_id,
    hv_prov_ord_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    src_vrsn_id,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_prov_ord_id,
    vdr_prov_ord_id_qual,
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
    prov_ord_dt,
    COALESCE(ordg_prov_nucc_taxnmy_cd, ordg_prov_alt_taxnmy_id),
    COALESCE(ordg_prov_mdcr_speclty_cd, ordg_prov_alt_speclty_id),
    ordg_prov_state_cd,
    ordg_prov_zip_cd,
    prov_ord_start_dt,
    prov_ord_end_dt,
    prov_ord_ctgy_cd,
    prov_ord_ctgy_cd_qual,
    prov_ord_ctgy_nm,
    prov_ord_ctgy_desc,
    prov_ord_typ_cd,
    prov_ord_typ_cd_qual,
    prov_ord_typ_nm,
    prov_ord_typ_desc,
    prov_ord_cd,
    prov_ord_cd_qual,
    prov_ord_nm,
    prov_ord_desc,
    prov_ord_alt_cd,
    prov_ord_alt_cd_qual,
    prov_ord_alt_nm,
    prov_ord_alt_desc,
    prov_ord_diag_cd,
    prov_ord_diag_cd_qual,
    prov_ord_diag_nm,
    prov_ord_diag_desc,
    prov_ord_snomed_cd,
    prov_ord_vcx_cd,
    prov_ord_vcx_cd_qual,
    prov_ord_vcx_nm,
    prov_ord_vcx_desc,
    prov_ord_rsn_cd,
    prov_ord_rsn_cd_qual,
    prov_ord_rsn_nm,
    prov_ord_rsn_desc,
    prov_ord_stat_cd,
    prov_ord_stat_cd_qual,
    prov_ord_stat_nm,
    prov_ord_stat_desc,
    prov_ord_complt_flg,
    prov_ord_complt_dt,
    prov_ord_complt_rsn_cd,
    prov_ord_complt_rsn_cd_qual,
    prov_ord_complt_rsn_nm,
    prov_ord_complt_rsn_desc,
    prov_ord_cxld_rsn_cd,
    prov_ord_cxld_rsn_cd_qual,
    prov_ord_cxld_rsn_nm,
    prov_ord_cxld_rsn_desc,
    prov_ord_cxld_dt,
    prov_ord_result_cd,
    prov_ord_result_cd_qual,
    prov_ord_result_nm,
    prov_ord_result_desc,
    prov_ord_result_rcvd_dt,
    prov_ord_trtmt_typ_cd,
    prov_ord_trtmt_typ_cd_qual,
    prov_ord_trtmt_typ_nm,
    prov_ord_trtmt_typ_desc,
    prov_ord_rfrd_speclty_cd,
    prov_ord_rfrd_speclty_cd_qual,
    prov_ord_rfrd_speclty_nm,
    prov_ord_rfrd_speclty_desc,
    prov_ord_specl_instrs_cd,
    prov_ord_specl_instrs_cd_qual,
    prov_ord_specl_instrs_nm,
    prov_ord_specl_instrs_desc,
    prov_ord_edctn_flg,
    prov_ord_edctn_dt,
    prov_ord_edctn_cd,
    prov_ord_edctn_cd_qual,
    prov_ord_edctn_nm,
    prov_ord_edctn_desc,
    data_captr_dt,
    rec_stat_cd,
    part_hvm_vdr_feed_id,
    part_mth
FROM dw.emr_prov_ord
    ;