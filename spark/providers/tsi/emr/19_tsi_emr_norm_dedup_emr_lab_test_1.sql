SELECT  
    hv_lab_test_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_lab_ord_id,
    vdr_lab_ord_id_qual,
    vdr_lab_test_id,
    vdr_lab_test_id_qual,
    hvid,
    ptnt_birth_yr,
    ptnt_gender_cd,
    ptnt_state_cd,
    ptnt_zip3_cd,
    hv_enc_id,
    enc_dt,
    lab_ord_dt,
    lab_test_smpl_collctn_dt,
    lab_test_prov_npi,
    lab_test_prov_qual,
    lab_test_prov_vdr_id,
    lab_test_prov_vdr_id_qual,
    lab_test_prov_mdcr_speclty_cd,
    lab_test_prov_alt_speclty_id,
    lab_test_prov_alt_speclty_id_qual,
    lab_test_prov_zip_cd,
    lab_test_loinc_cd,
    lab_result_nm,
    lab_result_msrmt,
    lab_result_uom,
    lab_result_ref_rng_txt,
    lab_ord_stat_cd,
    lab_ord_stat_cd_qual,
    lab_test_stat_cd,
    lab_test_stat_cd_qual,
    lab_test_grp_txt,
    data_captr_dt,
    prmy_src_tbl_nm,    
    part_hvm_vdr_feed_id,
    part_mth

FROM tsi_emr_norm_pre_emr_lab_test_1
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 
         31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41
