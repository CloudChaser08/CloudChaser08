from pyspark.sql.types import *

schema_v4 = StructType([
        StructField('row_id',                                    LongType(), True),
        StructField('hv_medctn_id',                              StringType(), True),
        StructField('crt_dt',                                    DateType(), True),
        StructField('mdl_vrsn_num',                              StringType(), True),
        StructField('data_set_nm',                               StringType(), True),
        StructField('src_vrsn_id',                               StringType(), True),
        StructField('hvm_vdr_id',                                IntegerType(), True),
        StructField('hvm_vdr_feed_id',                           IntegerType(), True),
        StructField('vdr_org_id',                                StringType(), True),
        StructField('vdr_medctn_ord_id',                         StringType(), True),
        StructField('vdr_medctn_ord_id_qual',                    StringType(), True),
        StructField('vdr_medctn_admin_id',                       StringType(), True),
        StructField('vdr_medctn_admin_id_qual',                  StringType(), True),
        StructField('hvid',                                      StringType(), True),
        StructField('ptnt_birth_yr',                             IntegerType(), True),
        StructField('ptnt_age_num',                              StringType(), True),
        StructField('ptnt_lvg_flg',                              StringType(), True),
        StructField('ptnt_dth_dt',                               DateType(), True),
        StructField('ptnt_gender_cd',                            StringType(), True),
        StructField('ptnt_state_cd',                             StringType(), True),
        StructField('ptnt_zip3_cd',                              StringType(), True),
        StructField('hv_enc_id',                                 StringType(), True),
        StructField('enc_dt',                                    DateType(), True),
        StructField('medctn_ord_dt',                             DateType(), True),
        StructField('medctn_admin_dt',                           DateType(), True),
        StructField('medctn_rndrg_fclty_npi',                    StringType(), True),
        StructField('medctn_rndrg_fclty_vdr_id',                 StringType(), True),
        StructField('medctn_rndrg_fclty_vdr_id_qual',            StringType(), True),
        StructField('medctn_rndrg_fclty_alt_id',                 StringType(), True),
        StructField('medctn_rndrg_fclty_alt_id_qual',            StringType(), True),
        StructField('medctn_rndrg_fclty_tax_id',                 StringType(), True),
        StructField('medctn_rndrg_fclty_dea_id',                 StringType(), True),
        StructField('medctn_rndrg_fclty_state_lic_id',           StringType(), True),
        StructField('medctn_rndrg_fclty_comrcl_id',              StringType(), True),
        StructField('medctn_rndrg_fclty_nucc_taxnmy_cd',         StringType(), True),
        StructField('medctn_rndrg_fclty_alt_taxnmy_id',          StringType(), True),
        StructField('medctn_rndrg_fclty_alt_taxnmy_id_qual',     StringType(), True),
        StructField('medctn_rndrg_fclty_mdcr_speclty_cd',        StringType(), True),
        StructField('medctn_rndrg_fclty_alt_speclty_id',         StringType(), True),
        StructField('medctn_rndrg_fclty_alt_speclty_id_qual',    StringType(), True),
        StructField('medctn_rndrg_fclty_nm',                     StringType(), True),
        StructField('medctn_rndrg_fclty_addr_1_txt',             StringType(), True),
        StructField('medctn_rndrg_fclty_addr_2_txt',             StringType(), True),
        StructField('medctn_rndrg_fclty_state_cd',               StringType(), True),
        StructField('medctn_rndrg_fclty_zip_cd',                 StringType(), True),
        StructField('medctn_ordg_prov_npi',                      StringType(), True),
        StructField('medctn_ordg_prov_vdr_id',                   StringType(), True),
        StructField('medctn_ordg_prov_vdr_id_qual',              StringType(), True),
        StructField('medctn_ordg_prov_alt_id',                   StringType(), True),
        StructField('medctn_ordg_prov_alt_id_qual',              StringType(), True),
        StructField('medctn_ordg_prov_tax_id',                   StringType(), True),
        StructField('medctn_ordg_prov_dea_id',                   StringType(), True),
        StructField('medctn_ordg_prov_state_lic_id',             StringType(), True),
        StructField('medctn_ordg_prov_comrcl_id',                StringType(), True),
        StructField('medctn_ordg_prov_upin',                     StringType(), True),
        StructField('medctn_ordg_prov_ssn',                      StringType(), True),
        StructField('medctn_ordg_prov_nucc_taxnmy_cd',           StringType(), True),
        StructField('medctn_ordg_prov_alt_taxnmy_id',            StringType(), True),
        StructField('medctn_ordg_prov_alt_taxnmy_id_qual',       StringType(), True),
        StructField('medctn_ordg_prov_mdcr_speclty_cd',          StringType(), True),
        StructField('medctn_ordg_prov_alt_speclty_id',           StringType(), True),
        StructField('medctn_ordg_prov_alt_speclty_id_qual',      StringType(), True),
        StructField('medctn_ordg_prov_frst_nm',                  StringType(), True),
        StructField('medctn_ordg_prov_last_nm',                  StringType(), True),
        StructField('medctn_ordg_prov_addr_1_txt',               StringType(), True),
        StructField('medctn_ordg_prov_addr_2_txt',               StringType(), True),
        StructField('medctn_ordg_prov_state_cd',                 StringType(), True),
        StructField('medctn_ordg_prov_zip_cd',                   StringType(), True),
        StructField('medctn_adminrg_fclty_npi',                  StringType(), True),
        StructField('medctn_adminrg_fclty_vdr_id',               StringType(), True),
        StructField('medctn_adminrg_fclty_vdr_id_qual',          StringType(), True),
        StructField('medctn_adminrg_fclty_alt_id',               StringType(), True),
        StructField('medctn_adminrg_fclty_alt_id_qual',          StringType(), True),
        StructField('medctn_adminrg_fclty_tax_id',               StringType(), True),
        StructField('medctn_adminrg_fclty_dea_id',               StringType(), True),
        StructField('medctn_adminrg_fclty_state_lic_id',         StringType(), True),
        StructField('medctn_adminrg_fclty_comrcl_id',            StringType(), True),
        StructField('medctn_adminrg_fclty_nucc_taxnmy_cd',       StringType(), True),
        StructField('medctn_adminrg_fclty_alt_taxnmy_id',        StringType(), True),
        StructField('medctn_adminrg_fclty_alt_taxnmy_id_qual',   StringType(), True),
        StructField('medctn_adminrg_fclty_mdcr_speclty_cd',      StringType(), True),
        StructField('medctn_adminrg_fclty_alt_speclty_id',       StringType(), True),
        StructField('medctn_adminrg_fclty_alt_speclty_id_qual',  StringType(), True),
        StructField('medctn_adminrg_fclty_nm',                   StringType(), True),
        StructField('medctn_adminrg_fclty_addr_1_txt',           StringType(), True),
        StructField('medctn_adminrg_fclty_addr_2_txt',           StringType(), True),
        StructField('medctn_adminrg_fclty_state_cd',             StringType(), True),
        StructField('medctn_adminrg_fclty_zip_cd',               StringType(), True),
        StructField('rx_num',                                    StringType(), True),
        StructField('medctn_start_dt',                           DateType(), True),
        StructField('medctn_end_dt',                             DateType(), True),
        StructField('medctn_diag_cd',                            StringType(), True),
        StructField('medctn_diag_cd_qual',                       StringType(), True),
        StructField('medctn_ndc',                                StringType(), True),
        StructField('medctn_lblr_cd',                            StringType(), True),
        StructField('medctn_drug_and_strth_cd',                  StringType(), True),
        StructField('medctn_pkg_cd',                             StringType(), True),
        StructField('medctn_hicl_thrptc_cls_cd',                 StringType(), True),
        StructField('medctn_hicl_cd',                            StringType(), True),
        StructField('medctn_gcn_cd',                             StringType(), True),
        StructField('medctn_rxnorm_cd',                          StringType(), True),
        StructField('medctn_snomed_cd',                          StringType(), True),
        StructField('medctn_genc_ok_flg',                        StringType(), True),
        StructField('medctn_brd_nm',                             StringType(), True),
        StructField('medctn_genc_nm',                            StringType(), True),
        StructField('medctn_rx_flg',                             StringType(), True),
        StructField('medctn_rx_qty',                             StringType(), True),
        StructField('medctn_dly_qty',                            StringType(), True),
        StructField('medctn_dispd_qty',                          StringType(), True),
        StructField('medctn_days_supply_qty',                    StringType(), True),
        StructField('medctn_admin_unt_qty',                      StringType(), True),
        StructField('medctn_admin_freq_qty',                     StringType(), True),
        StructField('medctn_admin_sched_cd',                     StringType(), True),
        StructField('medctn_admin_sched_qty',                    StringType(), True),
        StructField('medctn_admin_sig_cd',                       StringType(), True),
        StructField('medctn_admin_sig_txt',                      StringType(), True),
        StructField('medctn_admin_form_nm',                      StringType(), True),
        StructField('medctn_specl_pkgg_cd',                      StringType(), True),
        StructField('medctn_strth_txt',                          StringType(), True),
        StructField('medctn_strth_txt_qual',                     StringType(), True),
        StructField('medctn_dose_txt',                           StringType(), True),
        StructField('medctn_dose_txt_qual',                      StringType(), True),
        StructField('medctn_admin_rte_txt',                      StringType(), True),
        StructField('medctn_orig_rfll_qty',                      StringType(), True),
        StructField('medctn_fll_num',                            StringType(), True),
        StructField('medctn_remng_rfll_qty',                     StringType(), True),
        StructField('medctn_last_rfll_dt',                       DateType(), True),
        StructField('medctn_smpl_flg',                           StringType(), True),
        StructField('medctn_elect_rx_flg',                       StringType(), True),
        StructField('medctn_verfd_flg',                          StringType(), True),
        StructField('medctn_prod_svc_id',                        StringType(), True),
        StructField('medctn_prod_svc_id_qual',                   StringType(), True),
        StructField('data_captr_dt',                             DateType(), True),
        StructField('rec_stat_cd',                               StringType(), True),
        StructField('prmy_src_tbl_nm',                           StringType(), True)
])

schema_v6 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_medctn_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_medctn_ord_id', StringType(), True),
    StructField('vdr_medctn_ord_id_qual', StringType(), True),
    StructField('vdr_alt_medctn_ord_id', StringType(), True),
    StructField('vdr_alt_medctn_ord_id_qual', StringType(), True),
    StructField('vdr_medctn_admin_id', StringType(), True),
    StructField('vdr_medctn_admin_id_qual', StringType(), True),
    StructField('vdr_alt_medctn_admin_id', StringType(), True),
    StructField('vdr_alt_medctn_admin_id_qual', StringType(), True),
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
    StructField('medctn_ord_dt', DateType(), True),
    StructField('medctn_admin_dt', DateType(), True),
    StructField('medctn_rndrg_fclty_npi', StringType(), True),
    StructField('medctn_rndrg_fclty_vdr_id', StringType(), True),
    StructField('medctn_rndrg_fclty_vdr_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_id', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_tax_id', StringType(), True),
    StructField('medctn_rndrg_fclty_state_lic_id', StringType(), True),
    StructField('medctn_rndrg_fclty_comrcl_id', StringType(), True),
    StructField('medctn_rndrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_speclty_id', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_nm', StringType(), True),
    StructField('medctn_rndrg_fclty_addr_1_txt', StringType(), True),
    StructField('medctn_rndrg_fclty_addr_2_txt', StringType(), True),
    StructField('medctn_rndrg_fclty_state_cd', StringType(), True),
    StructField('medctn_rndrg_fclty_zip_cd', StringType(), True),
    StructField('medctn_ordg_prov_npi', StringType(), True),
    StructField('medctn_ordg_prov_vdr_id', StringType(), True),
    StructField('medctn_ordg_prov_vdr_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_alt_id', StringType(), True),
    StructField('medctn_ordg_prov_alt_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_tax_id', StringType(), True),
    StructField('medctn_ordg_prov_dea_id', StringType(), True),
    StructField('medctn_ordg_prov_state_lic_id', StringType(), True),
    StructField('medctn_ordg_prov_comrcl_id', StringType(), True),
    StructField('medctn_ordg_prov_upin', StringType(), True),
    StructField('medctn_ordg_prov_ssn', StringType(), True),
    StructField('medctn_ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('medctn_ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('medctn_ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('medctn_ordg_prov_alt_speclty_id', StringType(), True),
    StructField('medctn_ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_frst_nm', StringType(), True),
    StructField('medctn_ordg_prov_last_nm', StringType(), True),
    StructField('medctn_ordg_prov_addr_1_txt', StringType(), True),
    StructField('medctn_ordg_prov_addr_2_txt', StringType(), True),
    StructField('medctn_ordg_prov_state_cd', StringType(), True),
    StructField('medctn_ordg_prov_zip_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_npi', StringType(), True),
    StructField('medctn_adminrg_fclty_vdr_id', StringType(), True),
    StructField('medctn_adminrg_fclty_vdr_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_id', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_tax_id', StringType(), True),
    StructField('medctn_adminrg_fclty_state_lic_id', StringType(), True),
    StructField('medctn_adminrg_fclty_comrcl_id', StringType(), True),
    StructField('medctn_adminrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_speclty_id', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_nm', StringType(), True),
    StructField('medctn_adminrg_fclty_addr_1_txt', StringType(), True),
    StructField('medctn_adminrg_fclty_addr_2_txt', StringType(), True),
    StructField('medctn_adminrg_fclty_state_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_zip_cd', StringType(), True),
    StructField('rx_num', StringType(), True),
    StructField('medctn_start_dt', DateType(), True),
    StructField('medctn_end_dt', DateType(), True),
    StructField('medctn_ord_durtn_day_cnt', IntegerType(), True),
    StructField('medctn_ord_stat_cd', StringType(), True),
    StructField('medctn_ord_stat_cd_qual', StringType(), True),
    StructField('medctn_ord_cxld_dt', DateType(), True),
    StructField('medctn_ord_cxld_flg', StringType(), True),
    StructField('medctn_diag_cd', StringType(), True),
    StructField('medctn_diag_cd_qual', StringType(), True),
    StructField('medctn_ndc', StringType(), True),
    StructField('medctn_lblr_cd', StringType(), True),
    StructField('medctn_drug_and_strth_cd', StringType(), True),
    StructField('medctn_pkg_cd', StringType(), True),
    StructField('medctn_hicl_thrptc_cls_cd', StringType(), True),
    StructField('medctn_hicl_cd', StringType(), True),
    StructField('medctn_gcn_cd', StringType(), True),
    StructField('medctn_rxnorm_cd', StringType(), True),
    StructField('medctn_snomed_cd', StringType(), True),
    StructField('medctn_genc_ok_flg', StringType(), True),
    StructField('medctn_brd_nm', StringType(), True),
    StructField('medctn_genc_nm', StringType(), True),
    StructField('medctn_rx_flg', StringType(), True),
    StructField('medctn_extrnl_rx_flg', StringType(), True),
    StructField('medctn_rx_qty', StringType(), True),
    StructField('medctn_dly_qty', StringType(), True),
    StructField('medctn_dispd_qty', StringType(), True),
    StructField('medctn_days_supply_qty', StringType(), True),
    StructField('medctn_admin_unt_qty', StringType(), True),
    StructField('medctn_admin_freq_qty', StringType(), True),
    StructField('medctn_admin_sched_cd', StringType(), True),
    StructField('medctn_admin_sched_qty', StringType(), True),
    StructField('medctn_admin_sig_cd', StringType(), True),
    StructField('medctn_admin_form_nm', StringType(), True),
    StructField('medctn_specl_pkgg_cd', StringType(), True),
    StructField('medctn_strth_txt', StringType(), True),
    StructField('medctn_strth_txt_qual', StringType(), True),
    StructField('medctn_dose_txt', StringType(), True),
    StructField('medctn_dose_txt_qual', StringType(), True),
    StructField('medctn_dose_uom', StringType(), True),
    StructField('medctn_admin_rte_txt', StringType(), True),
    StructField('medctn_orig_rfll_qty', StringType(), True),
    StructField('medctn_fll_num', StringType(), True),
    StructField('medctn_remng_rfll_qty', StringType(), True),
    StructField('medctn_last_rfll_dt', DateType(), True),
    StructField('medctn_smpl_flg', StringType(), True),
    StructField('medctn_elect_rx_flg', StringType(), True),
    StructField('medctn_prod_svc_id', StringType(), True),
    StructField('medctn_prod_svc_id_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v7 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_medctn_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_medctn_ord_id', StringType(), True),
    StructField('vdr_medctn_ord_id_qual', StringType(), True),
    StructField('vdr_alt_medctn_ord_id', StringType(), True),
    StructField('vdr_alt_medctn_ord_id_qual', StringType(), True),
    StructField('vdr_medctn_admin_id', StringType(), True),
    StructField('vdr_medctn_admin_id_qual', StringType(), True),
    StructField('vdr_alt_medctn_admin_id', StringType(), True),
    StructField('vdr_alt_medctn_admin_id_qual', StringType(), True),
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
    StructField('medctn_ord_dt', DateType(), True),
    StructField('medctn_admin_dt', DateType(), True),
    StructField('medctn_rndrg_fclty_npi', StringType(), True),
    StructField('medctn_rndrg_fclty_vdr_id', StringType(), True),
    StructField('medctn_rndrg_fclty_vdr_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_id', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_tax_id', StringType(), True),
    StructField('medctn_rndrg_fclty_dea_id', StringType(), True),
    StructField('medctn_rndrg_fclty_state_lic_id', StringType(), True),
    StructField('medctn_rndrg_fclty_comrcl_id', StringType(), True),
    StructField('medctn_rndrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_speclty_id', StringType(), True),
    StructField('medctn_rndrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('medctn_rndrg_fclty_nm', StringType(), True),
    StructField('medctn_rndrg_fclty_addr_1_txt', StringType(), True),
    StructField('medctn_rndrg_fclty_addr_2_txt', StringType(), True),
    StructField('medctn_rndrg_fclty_state_cd', StringType(), True),
    StructField('medctn_rndrg_fclty_zip_cd', StringType(), True),
    StructField('medctn_ordg_prov_npi', StringType(), True),
    StructField('medctn_ordg_prov_vdr_id', StringType(), True),
    StructField('medctn_ordg_prov_vdr_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_alt_id', StringType(), True),
    StructField('medctn_ordg_prov_alt_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_tax_id', StringType(), True),
    StructField('medctn_ordg_prov_dea_id', StringType(), True),
    StructField('medctn_ordg_prov_state_lic_id', StringType(), True),
    StructField('medctn_ordg_prov_comrcl_id', StringType(), True),
    StructField('medctn_ordg_prov_upin', StringType(), True),
    StructField('medctn_ordg_prov_ssn', StringType(), True),
    StructField('medctn_ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('medctn_ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('medctn_ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('medctn_ordg_prov_alt_speclty_id', StringType(), True),
    StructField('medctn_ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('medctn_ordg_prov_frst_nm', StringType(), True),
    StructField('medctn_ordg_prov_last_nm', StringType(), True),
    StructField('medctn_ordg_prov_addr_1_txt', StringType(), True),
    StructField('medctn_ordg_prov_addr_2_txt', StringType(), True),
    StructField('medctn_ordg_prov_state_cd', StringType(), True),
    StructField('medctn_ordg_prov_zip_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_npi', StringType(), True),
    StructField('medctn_adminrg_fclty_vdr_id', StringType(), True),
    StructField('medctn_adminrg_fclty_vdr_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_id', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_tax_id', StringType(), True),
    StructField('medctn_adminrg_fclty_dea_id', StringType(), True),
    StructField('medctn_adminrg_fclty_state_lic_id', StringType(), True),
    StructField('medctn_adminrg_fclty_comrcl_id', StringType(), True),
    StructField('medctn_adminrg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_speclty_id', StringType(), True),
    StructField('medctn_adminrg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('medctn_adminrg_fclty_nm', StringType(), True),
    StructField('medctn_adminrg_fclty_addr_1_txt', StringType(), True),
    StructField('medctn_adminrg_fclty_addr_2_txt', StringType(), True),
    StructField('medctn_adminrg_fclty_state_cd', StringType(), True),
    StructField('medctn_adminrg_fclty_zip_cd', StringType(), True),
    StructField('rx_num', StringType(), True),
    StructField('medctn_start_dt', DateType(), True),
    StructField('medctn_end_dt', DateType(), True),
    StructField('medctn_ord_durtn_day_cnt', IntegerType(), True),
    StructField('medctn_ord_stat_cd', StringType(), True),
    StructField('medctn_ord_stat_cd_qual', StringType(), True),
    StructField('medctn_ord_cxld_dt', DateType(), True),
    StructField('medctn_ord_cxld_flg', StringType(), True),
    StructField('medctn_diag_cd', StringType(), True),
    StructField('medctn_diag_cd_qual', StringType(), True),
    StructField('medctn_ndc', StringType(), True),
    StructField('medctn_lblr_cd', StringType(), True),
    StructField('medctn_drug_and_strth_cd', StringType(), True),
    StructField('medctn_pkg_cd', StringType(), True),
    StructField('medctn_alt_substc_cd', StringType(), True),
    StructField('medctn_alt_substc_cd_qual', StringType(), True),
    StructField('medctn_hicl_thrptc_cls_cd', StringType(), True),
    StructField('medctn_hicl_cd', StringType(), True),
    StructField('medctn_gcn_cd', StringType(), True),
    StructField('medctn_rxnorm_cd', StringType(), True),
    StructField('medctn_snomed_cd', StringType(), True),
    StructField('medctn_genc_ok_flg', StringType(), True),
    StructField('medctn_brd_nm', StringType(), True),
    StructField('medctn_genc_nm', StringType(), True),
    StructField('medctn_rx_flg', StringType(), True),
    StructField('medctn_extrnl_rx_flg', StringType(), True),
    StructField('medctn_rx_qty', StringType(), True),
    StructField('medctn_dly_qty', StringType(), True),
    StructField('medctn_dispd_qty', StringType(), True),
    StructField('medctn_days_supply_qty', StringType(), True),
    StructField('medctn_admin_unt_qty', StringType(), True),
    StructField('medctn_admin_freq_qty', StringType(), True),
    StructField('medctn_admin_sched_cd', StringType(), True),
    StructField('medctn_admin_sched_qty', StringType(), True),
    StructField('medctn_admin_sig_cd', StringType(), True),
    StructField('medctn_admin_sig_txt', StringType(), True),
    StructField('medctn_admin_form_nm', StringType(), True),
    StructField('medctn_specl_pkgg_cd', StringType(), True),
    StructField('medctn_strth_txt', StringType(), True),
    StructField('medctn_strth_txt_qual', StringType(), True),
    StructField('medctn_dose_txt', StringType(), True),
    StructField('medctn_dose_txt_qual', StringType(), True),
    StructField('medctn_dose_uom', StringType(), True),
    StructField('medctn_admin_rte_txt', StringType(), True),
    StructField('medctn_orig_rfll_qty', StringType(), True),
    StructField('medctn_fll_num', StringType(), True),
    StructField('medctn_remng_rfll_qty', StringType(), True),
    StructField('medctn_last_rfll_dt', DateType(), True),
    StructField('medctn_smpl_flg', StringType(), True),
    StructField('medctn_elect_rx_flg', StringType(), True),
    StructField('medctn_verfd_flg', StringType(), True),
    StructField('medctn_prod_svc_id', StringType(), True),
    StructField('medctn_prod_svc_id_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])
