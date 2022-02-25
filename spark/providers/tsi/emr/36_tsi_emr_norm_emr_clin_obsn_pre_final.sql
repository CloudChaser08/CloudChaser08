SELECT  
    hv_clin_obsn_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_clin_obsn_id,
    vdr_clin_obsn_id_qual,
    hvid,
    ptnt_birth_yr,
    ptnt_gender_cd,
    ptnt_state_cd,
    ptnt_zip3_cd,
    hv_enc_id,
    enc_dt,
    clin_obsn_dt,
    clin_obsn_prov_npi,
    clin_obsn_prov_qual,
    clin_obsn_prov_vdr_id,
    clin_obsn_prov_vdr_id_qual,
    clin_obsn_prov_mdcr_speclty_cd,
    clin_obsn_prov_alt_speclty_id,
    clin_obsn_prov_alt_speclty_id_qual,
    clin_obsn_prov_zip_cd,
    clin_obsn_onset_dt,
    clin_obsn_resltn_dt,
    clin_obsn_typ_cd,
    clin_obsn_alt_cd,
    clin_obsn_alt_cd_qual,
    clin_obsn_msrmt,
    clin_obsn_uom,
    clin_obsn_grp_txt,
    data_src_cd,
    data_captr_dt,
    prmy_src_tbl_nm,
    part_hvm_vdr_feed_id,
    part_mth
FROM 
(
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_1
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_2
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_3
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_4
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_5
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_6
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_7
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_8
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_9
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_10
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_11
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_12
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_13
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_14
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_clin_obsn_15
)
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
         31, 32, 33, 34, 35, 36, 37, 38
