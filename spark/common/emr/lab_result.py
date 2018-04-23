from pyspark.sql.types import *

schema_v6 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_lab_result_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_lab_test_id', StringType(), True),
    StructField('vdr_lab_test_id_qual', StringType(), True),
    StructField('vdr_alt_lab_test_id', StringType(), True),
    StructField('vdr_alt_lab_test_id_qual', StringType(), True),
    StructField('vdr_lab_result_id', StringType(), True),
    StructField('vdr_lab_result_id_qual', StringType(), True),
    StructField('vdr_alt_lab_result_id', StringType(), True),
    StructField('vdr_alt_lab_result_id_qual', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_lvg_flg', StringType(), True),
    StructField('ptnt_dth_dt', DateType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_dt', DateType(), True),
    StructField('hv_lab_ord_id', StringType(), True),
    StructField('lab_test_smpl_collctn_dt', DateType(), True),
    StructField('lab_test_schedd_dt', DateType(), True),
    StructField('lab_test_execd_dt', DateType(), True),
    StructField('lab_result_dt', DateType(), True),
    StructField('lab_test_ordg_prov_npi', StringType(), True),
    StructField('lab_test_ordg_prov_vdr_id', StringType(), True),
    StructField('lab_test_ordg_prov_vdr_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_alt_id', StringType(), True),
    StructField('lab_test_ordg_prov_alt_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_tax_id', StringType(), True),
    StructField('lab_test_ordg_prov_state_lic_id', StringType(), True),
    StructField('lab_test_ordg_prov_comrcl_id', StringType(), True),
    StructField('lab_test_ordg_prov_upin', StringType(), True),
    StructField('lab_test_ordg_prov_ssn', StringType(), True),
    StructField('lab_test_ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('lab_test_ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('lab_test_ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('lab_test_ordg_prov_alt_speclty_id', StringType(), True),
    StructField('lab_test_ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_fclty_nm', StringType(), True),
    StructField('lab_test_ordg_prov_frst_nm', StringType(), True),
    StructField('lab_test_ordg_prov_last_nm', StringType(), True),
    StructField('lab_test_ordg_prov_addr_1_txt', StringType(), True),
    StructField('lab_test_ordg_prov_addr_2_txt', StringType(), True),
    StructField('lab_test_ordg_prov_state_cd', StringType(), True),
    StructField('lab_test_ordg_prov_zip_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_npi', StringType(), True),
    StructField('lab_test_exectg_fclty_vdr_id', StringType(), True),
    StructField('lab_test_exectg_fclty_vdr_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_id', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_tax_id', StringType(), True),
    StructField('lab_test_exectg_fclty_state_lic_id', StringType(), True),
    StructField('lab_test_exectg_fclty_comrcl_id', StringType(), True),
    StructField('lab_test_exectg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_speclty_id', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_nm', StringType(), True),
    StructField('lab_test_exectg_fclty_addr_1_txt', StringType(), True),
    StructField('lab_test_exectg_fclty_addr_2_txt', StringType(), True),
    StructField('lab_test_exectg_fclty_state_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_zip_cd', StringType(), True),
    StructField('lab_test_specmn_typ_cd', StringType(), True),
    StructField('lab_test_fstg_stat_flg', StringType(), True),
    StructField('lab_test_panel_nm', StringType(), True),
    StructField('lab_test_nm', StringType(), True),
    StructField('lab_test_desc', StringType(), True),
    StructField('lab_test_loinc_cd', StringType(), True),
    StructField('lab_test_snomed_cd', StringType(), True),
    StructField('lab_test_vdr_cd', StringType(), True),
    StructField('lab_test_vdr_cd_qual', StringType(), True),
    StructField('lab_test_alt_cd', StringType(), True),
    StructField('lab_test_alt_cd_qual', StringType(), True),
    StructField('lab_result_nm', StringType(), True),
    StructField('lab_result_desc', StringType(), True),
    StructField('lab_result_msrmt', StringType(), True),
    StructField('lab_result_uom', StringType(), True),
    StructField('lab_result_qual', StringType(), True),
    StructField('lab_result_abnorm_flg', StringType(), True),
    StructField('lab_result_norm_min_msrmt', StringType(), True),
    StructField('lab_result_norm_max_msrmt', StringType(), True),
    StructField('lab_test_diag_cd', StringType(), True),
    StructField('lab_test_diag_cd_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True),
])

schema_v7 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_lab_result_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_lab_test_id', StringType(), True),
    StructField('vdr_lab_test_id_qual', StringType(), True),
    StructField('vdr_alt_lab_test_id', StringType(), True),
    StructField('vdr_alt_lab_test_id_qual', StringType(), True),
    StructField('vdr_lab_result_id', StringType(), True),
    StructField('vdr_lab_result_id_qual', StringType(), True),
    StructField('vdr_alt_lab_result_id', StringType(), True),
    StructField('vdr_alt_lab_result_id_qual', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_lvg_flg', StringType(), True),
    StructField('ptnt_dth_dt', DateType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_dt', DateType(), True),
    StructField('hv_lab_ord_id', StringType(), True),
    StructField('lab_test_smpl_collctn_dt', DateType(), True),
    StructField('lab_test_schedd_dt', DateType(), True),
    StructField('lab_test_execd_dt', DateType(), True),
    StructField('lab_result_dt', DateType(), True),
    StructField('lab_test_ordg_prov_npi', StringType(), True),
    StructField('lab_test_ordg_prov_vdr_id', StringType(), True),
    StructField('lab_test_ordg_prov_vdr_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_alt_id', StringType(), True),
    StructField('lab_test_ordg_prov_alt_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_tax_id', StringType(), True),
    StructField('lab_test_ordg_prov_dea_id', StringType(), True),
    StructField('lab_test_ordg_prov_state_lic_id', StringType(), True),
    StructField('lab_test_ordg_prov_comrcl_id', StringType(), True),
    StructField('lab_test_ordg_prov_upin', StringType(), True),
    StructField('lab_test_ordg_prov_ssn', StringType(), True),
    StructField('lab_test_ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('lab_test_ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('lab_test_ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('lab_test_ordg_prov_alt_speclty_id', StringType(), True),
    StructField('lab_test_ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('lab_test_ordg_prov_fclty_nm', StringType(), True),
    StructField('lab_test_ordg_prov_frst_nm', StringType(), True),
    StructField('lab_test_ordg_prov_last_nm', StringType(), True),
    StructField('lab_test_ordg_prov_addr_1_txt', StringType(), True),
    StructField('lab_test_ordg_prov_addr_2_txt', StringType(), True),
    StructField('lab_test_ordg_prov_state_cd', StringType(), True),
    StructField('lab_test_ordg_prov_zip_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_npi', StringType(), True),
    StructField('lab_test_exectg_fclty_vdr_id', StringType(), True),
    StructField('lab_test_exectg_fclty_vdr_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_id', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_tax_id', StringType(), True),
    StructField('lab_test_exectg_fclty_dea_id', StringType(), True),
    StructField('lab_test_exectg_fclty_state_lic_id', StringType(), True),
    StructField('lab_test_exectg_fclty_comrcl_id', StringType(), True),
    StructField('lab_test_exectg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_speclty_id', StringType(), True),
    StructField('lab_test_exectg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('lab_test_exectg_fclty_nm', StringType(), True),
    StructField('lab_test_exectg_fclty_addr_1_txt', StringType(), True),
    StructField('lab_test_exectg_fclty_addr_2_txt', StringType(), True),
    StructField('lab_test_exectg_fclty_state_cd', StringType(), True),
    StructField('lab_test_exectg_fclty_zip_cd', StringType(), True),
    StructField('lab_test_specmn_typ_cd', StringType(), True),
    StructField('lab_test_fstg_stat_flg', StringType(), True),
    StructField('lab_test_panel_nm', StringType(), True),
    StructField('lab_test_nm', StringType(), True),
    StructField('lab_test_desc', StringType(), True),
    StructField('lab_test_loinc_cd', StringType(), True),
    StructField('lab_test_snomed_cd', StringType(), True),
    StructField('lab_test_vdr_cd', StringType(), True),
    StructField('lab_test_vdr_cd_qual', StringType(), True),
    StructField('lab_test_alt_cd', StringType(), True),
    StructField('lab_test_alt_cd_qual', StringType(), True),
    StructField('lab_result_nm', StringType(), True),
    StructField('lab_result_desc', StringType(), True),
    StructField('lab_result_msrmt', StringType(), True),
    StructField('lab_result_uom', StringType(), True),
    StructField('lab_result_qual', StringType(), True),
    StructField('lab_result_abnorm_flg', StringType(), True),
    StructField('lab_result_norm_min_msrmt', StringType(), True),
    StructField('lab_result_norm_max_msrmt', StringType(), True),
    StructField('lab_test_diag_cd', StringType(), True),
    StructField('lab_test_diag_cd_qual', StringType(), True),
    StructField('lab_result_stat_cd', StringType(), True),
    StructField('lab_result_stat_cd_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])
