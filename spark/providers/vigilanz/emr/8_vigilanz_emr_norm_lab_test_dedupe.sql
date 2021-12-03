SELECT  

hv_lab_test_id,
crt_dt,
mdl_vrsn_num,
data_set_nm,
hvm_vdr_id,
hvm_vdr_feed_id,
vdr_org_id,
vdr_lab_test_id,
vdr_lab_test_id_qual,
hvid,
ptnt_birth_yr,
ptnt_gender_cd,
ptnt_zip3_cd,
hv_enc_id,
enc_dt,
lab_test_smpl_collctn_dt,
lab_result_dt,
lab_test_specmn_typ_cd,
lab_test_vdr_id,
lab_result_nm,
lab_result_msrmt,
lab_result_uom,
lab_result_abnorm_flg,
lab_result_ref_rng_txt,
prmy_src_tbl_nm,
part_hvm_vdr_feed_id,
part_mth

FROM
(
SELECT  *
FROM    vigilanz_emr_norm_pre_emr_lab_test_1 
UNION ALL
SELECT  *
FROM    vigilanz_emr_norm_pre_emr_lab_test_2
)
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27
--limit 1
