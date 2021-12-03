SELECT  

hv_medctn_id,
crt_dt,
mdl_vrsn_num,
data_set_nm,
hvm_vdr_id,
hvm_vdr_feed_id,
vdr_org_id,
vdr_medctn_admin_id,
vdr_medctn_admin_id_qual,
hvid,
ptnt_birth_yr,
ptnt_gender_cd,
ptnt_zip3_cd,
hv_enc_id,
enc_dt,
medctn_admin_dt,
medctn_start_dt,
medctn_end_dt,
medctn_ord_stat_cd,
medctn_ord_stat_cd_qual,
medctn_ndc,
medctn_genc_nm,
medctn_dose_txt,
medctn_dose_txt_qual,
medctn_dose_uom,
medctn_admin_rte_txt,
prmy_src_tbl_nm,
part_hvm_vdr_feed_id,
part_mth

FROM vigilanz_emr_norm_pre_emr_medctn_1

GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29
--limit 1
