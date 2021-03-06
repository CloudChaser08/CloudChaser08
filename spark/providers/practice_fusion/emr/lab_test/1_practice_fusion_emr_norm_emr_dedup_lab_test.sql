SELECT
    hv_lab_test_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_lab_ord_id,
    vdr_lab_ord_id_qual,
    hvid,
    ptnt_birth_yr,
    ptnt_gender_cd,
    ptnt_state_cd,
    ptnt_zip3_cd,
    hv_enc_id,
    --enc_dt,
    lab_test_execd_dt,
    lab_result_dt,
    lab_test_prov_vdr_id,
    lab_test_prov_vdr_id_qual,
    lab_test_prov_nucc_taxnmy_cd,
    lab_test_prov_alt_speclty_id,
    lab_test_prov_alt_speclty_id_qual,
    lab_test_prov_fclty_nm,
    lab_test_prov_state_cd,
    lab_test_prov_zip_cd,
    lab_test_loinc_cd,
    lab_result_msrmt,
    lab_result_uom,
    lab_test_stat_cd,
    lab_test_stat_cd_qual,
    data_captr_dt,
    prmy_src_tbl_nm,
    part_hvm_vdr_feed_id,
    part_mth
FROM practice_fusion_emr_norm_emr_pre_lab_test
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
         31, 32, 33, 34
