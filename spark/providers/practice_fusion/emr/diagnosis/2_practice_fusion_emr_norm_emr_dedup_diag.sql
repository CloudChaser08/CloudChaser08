SELECT
    hv_diag_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_diag_id,
    vdr_diag_id_qual,
    hvid,
    ptnt_birth_yr,
    ptnt_gender_cd,
    ptnt_state_cd,
    ptnt_zip3_cd,
    hv_enc_id,
    enc_dt,
    diag_dt,
    diag_prov_vdr_id,
    diag_prov_vdr_id_qual,
    diag_prov_nucc_taxnmy_cd,
    diag_prov_alt_speclty_id,
    diag_prov_alt_speclty_id_qual,
    diag_prov_state_cd,
    diag_prov_zip_cd,
    diag_onset_dt,
    diag_resltn_dt,
    diag_cd,
    diag_cd_qual,
    data_captr_dt,
    prmy_src_tbl_nm,
    part_hvm_vdr_feed_id,
    part_mth
FROM
(
SELECT * FROM practice_fusion_emr_norm_emr_pre_diag UNION ALL
SELECT * FROM practice_fusion_emr_norm_emr_pre_diag_2
)
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
         31, 32
