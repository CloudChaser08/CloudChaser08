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
ptnt_zip3_cd,
hv_enc_id,
enc_dt,
diag_cd,
diag_cd_qual,
prmy_src_tbl_nm,
part_hvm_vdr_feed_id,
part_mth

FROM vigilanz_emr_norm_pre_emr_diag_1

GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20
        -- limit 1
