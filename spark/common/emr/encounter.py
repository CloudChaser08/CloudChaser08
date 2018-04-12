from pyspark.sql.types import *

schema_v6 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_enc_id', StringType(), True),
    StructField('vdr_enc_id_qual', StringType(), True),
    StructField('vdr_alt_enc_id', StringType(), True),
    StructField('vdr_alt_enc_id_qual', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_lvg_flg', StringType(), True),
    StructField('ptnt_dth_dt', DateType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('enc_start_dt', DateType(), True),
    StructField('enc_end_dt', DateType(), True),
    StructField('enc_vst_typ_cd', StringType(), True),
    StructField('enc_rndrg_fclty_npi', StringType(), True),
    StructField('enc_rndrg_fclty_vdr_id', StringType(), True),
    StructField('enc_rndrg_fclty_vdr_id_qual', StringType(), True),
    StructField('enc_rndrg_fclty_alt_id', StringType(), True),
    StructField('enc_rndrg_fclty_alt_id_qual', StringType(), True),
    StructField('enc_rndrg_fclty_tax_id', StringType(), True),
    StructField('enc_rndrg_fclty_state_lic_id', StringType(), True),
    StructField('enc_rndrg_fclty_comrcl_id', StringType(), True),
    StructField('enc_rndrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('enc_rndrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('enc_rndrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('enc_rndrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('enc_rndrg_fclty_alt_speclty_id', StringType(), True),
    StructField('enc_rndrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('enc_rndrg_fclty_nm', StringType(), True),
    StructField('enc_rndrg_fclty_addr_1_txt', StringType(), True),
    StructField('enc_rndrg_fclty_addr_2_txt', StringType(), True),
    StructField('enc_rndrg_fclty_state_cd', StringType(), True),
    StructField('enc_rndrg_fclty_zip_cd', StringType(), True),
    StructField('enc_rndrg_prov_npi', StringType(), True),
    StructField('enc_rndrg_prov_vdr_id', StringType(), True),
    StructField('enc_rndrg_prov_vdr_id_qual', StringType(), True),
    StructField('enc_rndrg_prov_alt_id', StringType(), True),
    StructField('enc_rndrg_prov_alt_id_qual', StringType(), True),
    StructField('enc_rndrg_prov_tax_id', StringType(), True),
    StructField('enc_rndrg_prov_state_lic_id', StringType(), True),
    StructField('enc_rndrg_prov_comrcl_id', StringType(), True),
    StructField('enc_rndrg_prov_upin', StringType(), True),
    StructField('enc_rndrg_prov_ssn', StringType(), True),
    StructField('enc_rndrg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('enc_rndrg_prov_alt_taxnmy_id', StringType(), True),
    StructField('enc_rndrg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('enc_rndrg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('enc_rndrg_prov_alt_speclty_id', StringType(), True),
    StructField('enc_rndrg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('enc_rndrg_prov_frst_nm', StringType(), True),
    StructField('enc_rndrg_prov_last_nm', StringType(), True),
    StructField('enc_rndrg_prov_addr_1_txt', StringType(), True),
    StructField('enc_rndrg_prov_addr_2_txt', StringType(), True),
    StructField('enc_rndrg_prov_state_cd', StringType(), True),
    StructField('enc_rndrg_prov_zip_cd', StringType(), True),
    StructField('enc_rfrg_prov_npi', StringType(), True),
    StructField('enc_rfrg_prov_vdr_id', StringType(), True),
    StructField('enc_rfrg_prov_vdr_id_qual', StringType(), True),
    StructField('enc_rfrg_prov_alt_id', StringType(), True),
    StructField('enc_rfrg_prov_alt_id_qual', StringType(), True),
    StructField('enc_rfrg_prov_tax_id', StringType(), True),
    StructField('enc_rfrg_prov_state_lic_id', StringType(), True),
    StructField('enc_rfrg_prov_comrcl_id', StringType(), True),
    StructField('enc_rfrg_prov_upin', StringType(), True),
    StructField('enc_rfrg_prov_ssn', StringType(), True),
    StructField('enc_rfrg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('enc_rfrg_prov_alt_taxnmy_id', StringType(), True),
    StructField('enc_rfrg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('enc_rfrg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('enc_rfrg_prov_alt_speclty_id', StringType(), True),
    StructField('enc_rfrg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('enc_rfrg_prov_fclty_nm', StringType(), True),
    StructField('enc_rfrg_prov_frst_nm', StringType(), True),
    StructField('enc_rfrg_prov_last_nm', StringType(), True),
    StructField('enc_rfrg_prov_addr_1_txt', StringType(), True),
    StructField('enc_rfrg_prov_addr_2_txt', StringType(), True),
    StructField('enc_rfrg_prov_state_cd', StringType(), True),
    StructField('enc_rfrg_prov_zip_cd', StringType(), True),
    StructField('enc_typ_cd', StringType(), True),
    StructField('enc_typ_cd_qual', StringType(), True),
    StructField('enc_typ_nm', StringType(), True),
    StructField('enc_pos_cd', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])