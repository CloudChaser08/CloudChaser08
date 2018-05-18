from pyspark.sql.types import *

schema_v4 = StructType([
        StructField('row_id',                                LongType(), True),
        StructField('hv_proc_id',                            StringType(), True),
        StructField('crt_dt',                                DateType(), True),
        StructField('mdl_vrsn_num',                          StringType(), True),
        StructField('data_set_nm',                           StringType(), True),
        StructField('src_vrsn_id',                           StringType(), True),
        StructField('hvm_vdr_id',                            IntegerType(), True),
        StructField('hvm_vdr_feed_id',                       IntegerType(), True),
        StructField('vdr_org_id',                            StringType(), True),
        StructField('vdr_proc_id',                           StringType(), True),
        StructField('vdr_proc_id_qual',                      StringType(), True),
        StructField('hvid',                                  StringType(), True),
        StructField('ptnt_birth_yr',                         IntegerType(), True),
        StructField('ptnt_age_num',                          StringType(), True),
        StructField('ptnt_lvg_flg',                          StringType(), True),
        StructField('ptnt_dth_dt',                           DateType(), True),
        StructField('ptnt_gender_cd',                        StringType(), True),
        StructField('ptnt_state_cd',                         StringType(), True),
        StructField('ptnt_zip3_cd',                          StringType(), True),
        StructField('hv_enc_id',                             StringType(), True),
        StructField('enc_dt',                                DateType(), True),
        StructField('proc_dt',                               DateType(), True),
        StructField('proc_rndrg_fclty_npi',                  StringType(), True),
        StructField('proc_rndrg_fclty_vdr_id',               StringType(), True),
        StructField('proc_rndrg_fclty_vdr_id_qual',          StringType(), True),
        StructField('proc_rndrg_fclty_alt_id',               StringType(), True),
        StructField('proc_rndrg_fclty_alt_id_qual',          StringType(), True),
        StructField('proc_rndrg_fclty_tax_id',               StringType(), True),
        StructField('proc_rndrg_fclty_dea_id',               StringType(), True),
        StructField('proc_rndrg_fclty_state_lic_id',         StringType(), True),
        StructField('proc_rndrg_fclty_comrcl_id',            StringType(), True),
        StructField('proc_rndrg_fclty_nucc_taxnmy_cd',       StringType(), True),
        StructField('proc_rndrg_fclty_alt_taxnmy_id',        StringType(), True),
        StructField('proc_rndrg_fclty_alt_taxnmy_id_qual',   StringType(), True),
        StructField('proc_rndrg_fclty_mdcr_speclty_cd',      StringType(), True),
        StructField('proc_rndrg_fclty_alt_speclty_id',       StringType(), True),
        StructField('proc_rndrg_fclty_alt_speclty_id_qual',  StringType(), True),
        StructField('proc_rndrg_fclty_nm',                   StringType(), True),
        StructField('proc_rndrg_fclty_addr_1_txt',           StringType(), True),
        StructField('proc_rndrg_fclty_addr_2_txt',           StringType(), True),
        StructField('proc_rndrg_fclty_state_cd',             StringType(), True),
        StructField('proc_rndrg_fclty_zip_cd',               StringType(), True),
        StructField('proc_rndrg_prov_npi',                   StringType(), True),
        StructField('proc_rndrg_prov_vdr_id',                StringType(), True),
        StructField('proc_rndrg_prov_vdr_id_qual',           StringType(), True),
        StructField('proc_rndrg_prov_alt_id',                StringType(), True),
        StructField('proc_rndrg_prov_alt_id_qual',           StringType(), True),
        StructField('proc_rndrg_prov_tax_id',                StringType(), True),
        StructField('proc_rndrg_prov_dea_id',                StringType(), True),
        StructField('proc_rndrg_prov_state_lic_id',          StringType(), True),
        StructField('proc_rndrg_prov_comrcl_id',             StringType(), True),
        StructField('proc_rndrg_prov_upin',                  StringType(), True),
        StructField('proc_rndrg_prov_ssn',                   StringType(), True),
        StructField('proc_rndrg_prov_nucc_taxnmy_cd',        StringType(), True),
        StructField('proc_rndrg_prov_alt_taxnmy_id',         StringType(), True),
        StructField('proc_rndrg_prov_alt_taxnmy_id_qual',    StringType(), True),
        StructField('proc_rndrg_prov_mdcr_speclty_cd',       StringType(), True),
        StructField('proc_rndrg_prov_alt_speclty_id',        StringType(), True),
        StructField('proc_rndrg_prov_alt_speclty_id_qual',   StringType(), True),
        StructField('proc_rndrg_prov_frst_nm',               StringType(), True),
        StructField('proc_rndrg_prov_last_nm',               StringType(), True),
        StructField('proc_rndrg_prov_addr_1_txt',            StringType(), True),
        StructField('proc_rndrg_prov_addr_2_txt',            StringType(), True),
        StructField('proc_rndrg_prov_state_cd',              StringType(), True),
        StructField('proc_rndrg_prov_zip_cd',                StringType(), True),
        StructField('proc_cd',                               StringType(), True),
        StructField('proc_cd_qual',                          StringType(), True),
        StructField('proc_cd_1_modfr',                       StringType(), True),
        StructField('proc_cd_2_modfr',                       StringType(), True),
        StructField('proc_cd_3_modfr',                       StringType(), True),
        StructField('proc_cd_4_modfr',                       StringType(), True),
        StructField('proc_cd_modfr_qual',                    StringType(), True),
        StructField('proc_snomed_cd',                        StringType(), True),
        StructField('proc_prty_cd',                          StringType(), True),
        StructField('proc_prty_cd_qual',                     StringType(), True),
        StructField('proc_alt_cd',                           StringType(), True),
        StructField('proc_alt_cd_qual',                      StringType(), True),
        StructField('proc_pos_cd',                           StringType(), True),
        StructField('proc_unit_qty',                         StringType(), True),
        StructField('proc_uom',                              StringType(), True),
        StructField('proc_diag_cd',                          StringType(), True),
        StructField('proc_diag_cd_qual',                     StringType(), True),
        StructField('data_captr_dt',                         DateType(), True),
        StructField('rec_stat_cd',                           StringType(), True),
        StructField('prmy_src_tbl_nm',                       StringType(), True)
])

schema_v6 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_proc_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_proc_id', StringType(), True),
    StructField('vdr_proc_id_qual', StringType(), True),
    StructField('vdr_alt_proc_id', StringType(), True),
    StructField('vdr_alt_proc_id_qual', StringType(), True),
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
    StructField('proc_dt', DateType(), True),
    StructField('proc_rndrg_fclty_npi', StringType(), True),
    StructField('proc_rndrg_fclty_vdr_id', StringType(), True),
    StructField('proc_rndrg_fclty_vdr_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_alt_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_tax_id', StringType(), True),
    StructField('proc_rndrg_fclty_state_lic_id', StringType(), True),
    StructField('proc_rndrg_fclty_comrcl_id', StringType(), True),
    StructField('proc_rndrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('proc_rndrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('proc_rndrg_fclty_alt_speclty_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_nm', StringType(), True),
    StructField('proc_rndrg_fclty_addr_1_txt', StringType(), True),
    StructField('proc_rndrg_fclty_addr_2_txt', StringType(), True),
    StructField('proc_rndrg_fclty_state_cd', StringType(), True),
    StructField('proc_rndrg_fclty_zip_cd', StringType(), True),
    StructField('proc_rndrg_prov_npi', StringType(), True),
    StructField('proc_rndrg_prov_vdr_id', StringType(), True),
    StructField('proc_rndrg_prov_vdr_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_alt_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_tax_id', StringType(), True),
    StructField('proc_rndrg_prov_state_lic_id', StringType(), True),
    StructField('proc_rndrg_prov_comrcl_id', StringType(), True),
    StructField('proc_rndrg_prov_upin', StringType(), True),
    StructField('proc_rndrg_prov_ssn', StringType(), True),
    StructField('proc_rndrg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('proc_rndrg_prov_alt_taxnmy_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('proc_rndrg_prov_alt_speclty_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_frst_nm', StringType(), True),
    StructField('proc_rndrg_prov_last_nm', StringType(), True),
    StructField('proc_rndrg_prov_addr_1_txt', StringType(), True),
    StructField('proc_rndrg_prov_addr_2_txt', StringType(), True),
    StructField('proc_rndrg_prov_state_cd', StringType(), True),
    StructField('proc_rndrg_prov_zip_cd', StringType(), True),
    StructField('proc_cd', StringType(), True),
    StructField('proc_cd_qual', StringType(), True),
    StructField('proc_cd_1_modfr', StringType(), True),
    StructField('proc_cd_2_modfr', StringType(), True),
    StructField('proc_cd_3_modfr', StringType(), True),
    StructField('proc_cd_4_modfr', StringType(), True),
    StructField('proc_cd_modfr_qual', StringType(), True),
    StructField('proc_snomed_cd', StringType(), True),
    StructField('proc_prty_cd', StringType(), True),
    StructField('proc_prty_cd_qual', StringType(), True),
    StructField('proc_alt_cd', StringType(), True),
    StructField('proc_alt_cd_qual', StringType(), True),
    StructField('proc_pos_cd', StringType(), True),
    StructField('proc_unit_qty', StringType(), True),
    StructField('proc_uom', StringType(), True),
    StructField('proc_diag_cd', StringType(), True),
    StructField('proc_diag_cd_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v7 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_proc_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_proc_id', StringType(), True),
    StructField('vdr_proc_id_qual', StringType(), True),
    StructField('vdr_alt_proc_id', StringType(), True),
    StructField('vdr_alt_proc_id_qual', StringType(), True),
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
    StructField('proc_dt', DateType(), True),
    StructField('proc_rndrg_fclty_npi', StringType(), True),
    StructField('proc_rndrg_fclty_vdr_id', StringType(), True),
    StructField('proc_rndrg_fclty_vdr_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_alt_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_tax_id', StringType(), True),
    StructField('proc_rndrg_fclty_dea_id', StringType(), True),
    StructField('proc_rndrg_fclty_state_lic_id', StringType(), True),
    StructField('proc_rndrg_fclty_comrcl_id', StringType(), True),
    StructField('proc_rndrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('proc_rndrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('proc_rndrg_fclty_alt_speclty_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_nm', StringType(), True),
    StructField('proc_rndrg_fclty_addr_1_txt', StringType(), True),
    StructField('proc_rndrg_fclty_addr_2_txt', StringType(), True),
    StructField('proc_rndrg_fclty_state_cd', StringType(), True),
    StructField('proc_rndrg_fclty_zip_cd', StringType(), True),
    StructField('proc_rndrg_prov_npi', StringType(), True),
    StructField('proc_rndrg_prov_vdr_id', StringType(), True),
    StructField('proc_rndrg_prov_vdr_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_alt_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_tax_id', StringType(), True),
    StructField('proc_rndrg_prov_dea_id', StringType(), True),
    StructField('proc_rndrg_prov_state_lic_id', StringType(), True),
    StructField('proc_rndrg_prov_comrcl_id', StringType(), True),
    StructField('proc_rndrg_prov_upin', StringType(), True),
    StructField('proc_rndrg_prov_ssn', StringType(), True),
    StructField('proc_rndrg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('proc_rndrg_prov_alt_taxnmy_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('proc_rndrg_prov_alt_speclty_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_frst_nm', StringType(), True),
    StructField('proc_rndrg_prov_last_nm', StringType(), True),
    StructField('proc_rndrg_prov_addr_1_txt', StringType(), True),
    StructField('proc_rndrg_prov_addr_2_txt', StringType(), True),
    StructField('proc_rndrg_prov_state_cd', StringType(), True),
    StructField('proc_rndrg_prov_zip_cd', StringType(), True),
    StructField('proc_cd', StringType(), True),
    StructField('proc_cd_qual', StringType(), True),
    StructField('proc_cd_1_modfr', StringType(), True),
    StructField('proc_cd_2_modfr', StringType(), True),
    StructField('proc_cd_3_modfr', StringType(), True),
    StructField('proc_cd_4_modfr', StringType(), True),
    StructField('proc_cd_modfr_qual', StringType(), True),
    StructField('proc_snomed_cd', StringType(), True),
    StructField('proc_prty_cd', StringType(), True),
    StructField('proc_prty_cd_qual', StringType(), True),
    StructField('proc_alt_cd', StringType(), True),
    StructField('proc_alt_cd_qual', StringType(), True),
    StructField('proc_pos_cd', StringType(), True),
    StructField('proc_unit_qty', StringType(), True),
    StructField('proc_uom', StringType(), True),
    StructField('proc_diag_cd', StringType(), True),
    StructField('proc_diag_cd_qual', StringType(), True),
    StructField('proc_stat_cd', StringType(), True),
    StructField('proc_stat_cd_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v8 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_proc_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_proc_id', StringType(), True),
    StructField('vdr_proc_id_qual', StringType(), True),
    StructField('vdr_alt_proc_id', StringType(), True),
    StructField('vdr_alt_proc_id_qual', StringType(), True),
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
    StructField('proc_dt', DateType(), True),
    StructField('proc_rndrg_fclty_npi', StringType(), True),
    StructField('proc_rndrg_fclty_vdr_id', StringType(), True),
    StructField('proc_rndrg_fclty_vdr_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_alt_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_tax_id', StringType(), True),
    StructField('proc_rndrg_fclty_dea_id', StringType(), True),
    StructField('proc_rndrg_fclty_state_lic_id', StringType(), True),
    StructField('proc_rndrg_fclty_comrcl_id', StringType(), True),
    StructField('proc_rndrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('proc_rndrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('proc_rndrg_fclty_alt_speclty_id', StringType(), True),
    StructField('proc_rndrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('proc_rndrg_fclty_nm', StringType(), True),
    StructField('proc_rndrg_fclty_addr_1_txt', StringType(), True),
    StructField('proc_rndrg_fclty_addr_2_txt', StringType(), True),
    StructField('proc_rndrg_fclty_state_cd', StringType(), True),
    StructField('proc_rndrg_fclty_zip_cd', StringType(), True),
    StructField('proc_rndrg_prov_npi', StringType(), True),
    StructField('proc_rndrg_prov_vdr_id', StringType(), True),
    StructField('proc_rndrg_prov_vdr_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_alt_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_tax_id', StringType(), True),
    StructField('proc_rndrg_prov_dea_id', StringType(), True),
    StructField('proc_rndrg_prov_state_lic_id', StringType(), True),
    StructField('proc_rndrg_prov_comrcl_id', StringType(), True),
    StructField('proc_rndrg_prov_upin', StringType(), True),
    StructField('proc_rndrg_prov_ssn', StringType(), True),
    StructField('proc_rndrg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('proc_rndrg_prov_alt_taxnmy_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('proc_rndrg_prov_alt_speclty_id', StringType(), True),
    StructField('proc_rndrg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('proc_rndrg_prov_frst_nm', StringType(), True),
    StructField('proc_rndrg_prov_last_nm', StringType(), True),
    StructField('proc_rndrg_prov_addr_1_txt', StringType(), True),
    StructField('proc_rndrg_prov_addr_2_txt', StringType(), True),
    StructField('proc_rndrg_prov_state_cd', StringType(), True),
    StructField('proc_rndrg_prov_zip_cd', StringType(), True),
    StructField('proc_cd', StringType(), True),
    StructField('proc_cd_qual', StringType(), True),
    StructField('proc_cd_1_modfr', StringType(), True),
    StructField('proc_cd_2_modfr', StringType(), True),
    StructField('proc_cd_3_modfr', StringType(), True),
    StructField('proc_cd_4_modfr', StringType(), True),
    StructField('proc_cd_modfr_qual', StringType(), True),
    StructField('proc_snomed_cd', StringType(), True),
    StructField('proc_prty_cd', StringType(), True),
    StructField('proc_prty_cd_qual', StringType(), True),
    StructField('proc_alt_cd', StringType(), True),
    StructField('proc_alt_cd_qual', StringType(), True),
    StructField('proc_pos_cd', StringType(), True),
    StructField('proc_unit_qty', StringType(), True),
    StructField('proc_uom', StringType(), True),
    StructField('proc_diag_cd', StringType(), True),
    StructField('proc_diag_cd_qual', StringType(), True),
    StructField('proc_stat_cd', StringType(), True),
    StructField('proc_stat_cd_qual', StringType(), True),
    StructField('proc_typ_cd', StringType(), True),
    StructField('proc_typ_cd_qual', StringType(), True),
    StructField('proc_admin_rte_cd', StringType(), True),
    StructField('proc_admin_site_cd', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])
