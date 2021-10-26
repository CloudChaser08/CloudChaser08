SELECT  

hv_enc_id,
crt_dt,
mdl_vrsn_num,
data_set_nm,
hvm_vdr_id,
hvm_vdr_feed_id,
vdr_enc_id,
vdr_enc_id_qual,
hvid,
ptnt_birth_yr,
ptnt_gender_cd,
ptnt_state_cd,
ptnt_zip3_cd,
enc_start_dt,
enc_end_dt,
enc_prov_npi,
enc_prov_qual,
enc_prov_vdr_id,
enc_prov_vdr_id_qual,
enc_prov_mdcr_speclty_cd,
enc_prov_alt_speclty_id,
enc_prov_alt_speclty_id_qual,
enc_prov_zip_cd,
enc_typ_nm,
enc_stat_cd,
enc_stat_cd_qual,
enc_grp_txt,
data_captr_dt,
prmy_src_tbl_nm,
part_hvm_vdr_feed_id,
part_mth

FROM 
(
SELECT * FROM tsi_emr_norm_pre_emr_enc_1
UNION ALL
SELECT * FROM tsi_emr_norm_pre_emr_enc_2
)
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
