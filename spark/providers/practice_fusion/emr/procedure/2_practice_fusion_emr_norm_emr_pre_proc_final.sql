SELECT
    hv_proc_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_org_id,
    vdr_proc_id,
    vdr_proc_id_qual,
    hvid,
    ptnt_birth_yr,
    ptnt_gender_cd,
    ptnt_state_cd,
    ptnt_zip3_cd,
    hv_enc_id,
    enc_dt,
    proc_dt,
    proc_prov_vdr_id,
    proc_prov_vdr_id_qual,
    proc_prov_nucc_taxnmy_cd,
    proc_prov_alt_speclty_id,
    proc_prov_alt_speclty_id_qual,
    proc_prov_state_cd,
    proc_prov_zip_cd,
    proc_cd,
    proc_cd_qual,
    proc_snomed_cd,
    proc_alt_cd,
    proc_alt_cd_qual,
    proc_stat_cd,
    proc_stat_cd_qual,
    proc_typ_cd,
    proc_typ_cd_qual,
    data_captr_dt,
    prmy_src_tbl_nm,
    part_hvm_vdr_feed_id,
    part_mth
FROM
(
SELECT * FROM practice_fusion_emr_norm_emr_proc_1
UNION ALL
SELECT * FROM practice_fusion_emr_norm_emr_proc_2
)
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
         31, 32, 33, 34, 35, 36, 37
