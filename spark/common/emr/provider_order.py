from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

schema_v4 = StructType([
        StructField('row_id', LongType(), True),
        StructField('hv_prov_ord_id', StringType(), True),
        StructField('crt_dt', DateType(), True),
        StructField('mdl_vrsn_num', StringType(), True),
        StructField('data_set_nm', StringType(), True),
        StructField('src_vrsn_id', StringType(), True),
        StructField('hvm_vdr_id', IntegerType(), True),
        StructField('hvm_vdr_feed_id', IntegerType(), True),
        StructField('vdr_org_id', StringType(), True),
        StructField('vdr_prov_ord_id', StringType(), True),
        StructField('vdr_prov_ord_id_qual', StringType(), True),
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
        StructField('prov_ord_dt', DateType(), True),
        StructField('ordg_prov_npi', StringType(), True),
        StructField('ordg_prov_vdr_id', StringType(), True),
        StructField('ordg_prov_vdr_id_qual', StringType(), True),
        StructField('ordg_prov_alt_id', StringType(), True),
        StructField('ordg_prov_alt_id_qual', StringType(), True),
        StructField('ordg_prov_tax_id', StringType(), True),
        StructField('ordg_prov_dea_id', StringType(), True),
        StructField('ordg_prov_state_lic_id', StringType(), True),
        StructField('ordg_prov_comrcl_id', StringType(), True),
        StructField('ordg_prov_upin', StringType(), True),
        StructField('ordg_prov_ssn', StringType(), True),
        StructField('ordg_prov_nucc_taxnmy_cd', StringType(), True),
        StructField('ordg_prov_alt_taxnmy_id', StringType(), True),
        StructField('ordg_prov_alt_taxnmy_id_qual', StringType(), True),
        StructField('ordg_prov_mdcr_speclty_cd', StringType(), True),
        StructField('ordg_prov_alt_speclty_id', StringType(), True),
        StructField('ordg_prov_alt_speclty_id_qual', StringType(), True),
        StructField('ordg_prov_fclty_nm', StringType(), True),
        StructField('ordg_prov_frst_nm', StringType(), True),
        StructField('ordg_prov_last_nm', StringType(), True),
        StructField('ordg_prov_addr_1_txt', StringType(), True),
        StructField('ordg_prov_addr_2_txt', StringType(), True),
        StructField('ordg_prov_state_cd', StringType(), True),
        StructField('ordg_prov_zip_cd', StringType(), True),
        StructField('prov_ord_start_dt', DateType(), True),
        StructField('prov_ord_end_dt', DateType(), True),
        StructField('prov_ord_ctgy_cd', StringType(), True),
        StructField('prov_ord_ctgy_cd_qual', StringType(), True),
        StructField('prov_ord_ctgy_nm', StringType(), True),
        StructField('prov_ord_ctgy_desc', StringType(), True),
        StructField('prov_ord_typ_cd', StringType(), True),
        StructField('prov_ord_typ_cd_qual', StringType(), True),
        StructField('prov_ord_typ_nm', StringType(), True),
        StructField('prov_ord_typ_desc', StringType(), True),
        StructField('prov_ord_cd', StringType(), True),
        StructField('prov_ord_cd_qual', StringType(), True),
        StructField('prov_ord_nm', StringType(), True),
        StructField('prov_ord_desc', StringType(), True),
        StructField('prov_ord_alt_cd', StringType(), True),
        StructField('prov_ord_alt_cd_qual', StringType(), True),
        StructField('prov_ord_alt_nm', StringType(), True),
        StructField('prov_ord_alt_desc', StringType(), True),
        StructField('prov_ord_diag_cd', StringType(), True),
        StructField('prov_ord_diag_cd_qual', StringType(), True),
        StructField('prov_ord_diag_nm', StringType(), True),
        StructField('prov_ord_diag_desc', StringType(), True),
        StructField('prov_ord_snomed_cd', StringType(), True),
        StructField('prov_ord_vcx_cd', StringType(), True),
        StructField('prov_ord_vcx_cd_qual', StringType(), True),
        StructField('prov_ord_vcx_nm', StringType(), True),
        StructField('prov_ord_vcx_desc', StringType(), True),
        StructField('prov_ord_rsn_cd', StringType(), True),
        StructField('prov_ord_rsn_cd_qual', StringType(), True),
        StructField('prov_ord_rsn_nm', StringType(), True),
        StructField('prov_ord_rsn_desc', StringType(), True),
        StructField('prov_ord_stat_cd', StringType(), True),
        StructField('prov_ord_stat_cd_qual', StringType(), True),
        StructField('prov_ord_stat_nm', StringType(), True),
        StructField('prov_ord_stat_desc', StringType(), True),
        StructField('prov_ord_complt_flg', StringType(), True),
        StructField('prov_ord_complt_dt', DateType(), True),
        StructField('prov_ord_complt_rsn_cd', StringType(), True),
        StructField('prov_ord_complt_rsn_cd_qual', StringType(), True),
        StructField('prov_ord_complt_rsn_nm', StringType(), True),
        StructField('prov_ord_complt_rsn_desc', StringType(), True),
        StructField('prov_ord_cxld_rsn_cd', StringType(), True),
        StructField('prov_ord_cxld_rsn_cd_qual', StringType(), True),
        StructField('prov_ord_cxld_rsn_nm', StringType(), True),
        StructField('prov_ord_cxld_rsn_desc', StringType(), True),
        StructField('prov_ord_cxld_dt', DateType(), True),
        StructField('prov_ord_result_cd', StringType(), True),
        StructField('prov_ord_result_cd_qual', StringType(), True),
        StructField('prov_ord_result_nm', StringType(), True),
        StructField('prov_ord_result_desc', StringType(), True),
        StructField('prov_ord_result_rcvd_dt', DateType(), True),
        StructField('prov_ord_trtmt_typ_cd', StringType(), True),
        StructField('prov_ord_trtmt_typ_cd_qual', StringType(), True),
        StructField('prov_ord_trtmt_typ_nm', StringType(), True),
        StructField('prov_ord_trtmt_typ_desc', StringType(), True),
        StructField('prov_ord_rfrd_speclty_cd', StringType(), True),
        StructField('prov_ord_rfrd_speclty_cd_qual', StringType(), True),
        StructField('prov_ord_rfrd_speclty_nm', StringType(), True),
        StructField('prov_ord_rfrd_speclty_desc', StringType(), True),
        StructField('prov_ord_specl_instrs_cd', StringType(), True),
        StructField('prov_ord_specl_instrs_cd_qual', StringType(), True),
        StructField('prov_ord_specl_instrs_nm', StringType(), True),
        StructField('prov_ord_specl_instrs_desc', StringType(), True),
        StructField('prov_ord_edctn_flg', StringType(), True),
        StructField('prov_ord_edctn_dt', DateType(), True),
        StructField('prov_ord_edctn_cd', StringType(), True),
        StructField('prov_ord_edctn_cd_qual', StringType(), True),
        StructField('prov_ord_edctn_nm', StringType(), True),
        StructField('prov_ord_edctn_desc', StringType(), True),
        StructField('data_captr_dt', DateType(), True),
        StructField('rec_stat_cd', StringType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v6 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_prov_ord_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_prov_ord_id', StringType(), True),
    StructField('vdr_prov_ord_id_qual', StringType(), True),
    StructField('vdr_alt_prov_ord_id', StringType(), True),
    StructField('vdr_alt_prov_ord_id_qual', StringType(), True),
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
    StructField('prov_ord_dt', DateType(), True),
    StructField('ordg_prov_npi', StringType(), True),
    StructField('ordg_prov_vdr_id', StringType(), True),
    StructField('ordg_prov_vdr_id_qual', StringType(), True),
    StructField('ordg_prov_alt_id', StringType(), True),
    StructField('ordg_prov_alt_id_qual', StringType(), True),
    StructField('ordg_prov_tax_id', StringType(), True),
    StructField('ordg_prov_state_lic_id', StringType(), True),
    StructField('ordg_prov_comrcl_id', StringType(), True),
    StructField('ordg_prov_upin', StringType(), True),
    StructField('ordg_prov_ssn', StringType(), True),
    StructField('ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('ordg_prov_alt_speclty_id', StringType(), True),
    StructField('ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('ordg_prov_fclty_nm', StringType(), True),
    StructField('ordg_prov_frst_nm', StringType(), True),
    StructField('ordg_prov_last_nm', StringType(), True),
    StructField('ordg_prov_addr_1_txt', StringType(), True),
    StructField('ordg_prov_addr_2_txt', StringType(), True),
    StructField('ordg_prov_state_cd', StringType(), True),
    StructField('ordg_prov_zip_cd', StringType(), True),
    StructField('ordg_fclty_npi', StringType(), True),
    StructField('ordg_fclty_vdr_id', StringType(), True),
    StructField('ordg_fclty_vdr_id_qual', StringType(), True),
    StructField('ordg_fclty_alt_id', StringType(), True),
    StructField('ordg_fclty_alt_id_qual', StringType(), True),
    StructField('ordg_fclty_tax_id', StringType(), True),
    StructField('ordg_fclty_state_lic_id', StringType(), True),
    StructField('ordg_fclty_comrcl_id', StringType(), True),
    StructField('ordg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('ordg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('ordg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('ordg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('ordg_fclty_alt_speclty_id', StringType(), True),
    StructField('ordg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('ordg_fclty_nm', StringType(), True),
    StructField('ordg_fclty_addr_1_txt', StringType(), True),
    StructField('ordg_fclty_addr_2_txt', StringType(), True),
    StructField('ordg_fclty_state_cd', StringType(), True),
    StructField('ordg_fclty_zip_cd', StringType(), True),
    StructField('prov_ord_start_dt', DateType(), True),
    StructField('prov_ord_end_dt', DateType(), True),
    StructField('prov_ord_ctgy_cd', StringType(), True),
    StructField('prov_ord_ctgy_cd_qual', StringType(), True),
    StructField('prov_ord_ctgy_nm', StringType(), True),
    StructField('prov_ord_ctgy_desc', StringType(), True),
    StructField('prov_ord_typ_cd', StringType(), True),
    StructField('prov_ord_typ_cd_qual', StringType(), True),
    StructField('prov_ord_typ_nm', StringType(), True),
    StructField('prov_ord_typ_desc', StringType(), True),
    StructField('prov_ord_cd', StringType(), True),
    StructField('prov_ord_cd_qual', StringType(), True),
    StructField('prov_ord_nm', StringType(), True),
    StructField('prov_ord_desc', StringType(), True),
    StructField('prov_ord_alt_cd', StringType(), True),
    StructField('prov_ord_alt_cd_qual', StringType(), True),
    StructField('prov_ord_alt_nm', StringType(), True),
    StructField('prov_ord_alt_desc', StringType(), True),
    StructField('prov_ord_diag_cd', StringType(), True),
    StructField('prov_ord_diag_cd_qual', StringType(), True),
    StructField('prov_ord_diag_nm', StringType(), True),
    StructField('prov_ord_snomed_cd', StringType(), True),
    StructField('prov_ord_vcx_cd', StringType(), True),
    StructField('prov_ord_vcx_cd_qual', StringType(), True),
    StructField('prov_ord_vcx_nm', StringType(), True),
    StructField('prov_ord_rsn_cd', StringType(), True),
    StructField('prov_ord_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_rsn_nm', StringType(), True),
    StructField('prov_ord_rsn_desc', StringType(), True),
    StructField('prov_ord_stat_cd', StringType(), True),
    StructField('prov_ord_stat_cd_qual', StringType(), True),
    StructField('prov_ord_stat_nm', StringType(), True),
    StructField('prov_ord_stat_desc', StringType(), True),
    StructField('prov_ord_complt_flg', StringType(), True),
    StructField('prov_ord_complt_dt', DateType(), True),
    StructField('prov_ord_durtn_day_cnt', IntegerType(), True),
    StructField('prov_ord_complt_rsn_cd', StringType(), True),
    StructField('prov_ord_complt_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_complt_rsn_nm', StringType(), True),
    StructField('prov_ord_complt_rsn_desc', StringType(), True),
    StructField('prov_ord_cxld_rsn_cd', StringType(), True),
    StructField('prov_ord_cxld_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_cxld_rsn_nm', StringType(), True),
    StructField('prov_ord_cxld_rsn_desc', StringType(), True),
    StructField('prov_ord_cxld_dt', DateType(), True),
    StructField('prov_ord_cxld_flg', StringType(), True),
    StructField('prov_ord_result_cd', StringType(), True),
    StructField('prov_ord_result_cd_qual', StringType(), True),
    StructField('prov_ord_result_nm', StringType(), True),
    StructField('prov_ord_result_desc', StringType(), True),
    StructField('prov_ord_result_rcvd_dt', DateType(), True),
    StructField('prov_ord_trtmt_typ_cd', StringType(), True),
    StructField('prov_ord_trtmt_typ_cd_qual', StringType(), True),
    StructField('prov_ord_trtmt_typ_nm', StringType(), True),
    StructField('prov_ord_rfrd_speclty_cd', StringType(), True),
    StructField('prov_ord_rfrd_speclty_cd_qual', StringType(), True),
    StructField('prov_ord_rfrd_speclty_nm', StringType(), True),
    StructField('prov_ord_rfrd_speclty_desc', StringType(), True),
    StructField('prov_ord_specl_instrs_cd', StringType(), True),
    StructField('prov_ord_specl_instrs_cd_qual', StringType(), True),
    StructField('prov_ord_specl_instrs_nm', StringType(), True),
    StructField('prov_ord_specl_instrs_desc', StringType(), True),
    StructField('prov_ord_edctn_flg', StringType(), True),
    StructField('prov_ord_edctn_dt', DateType(), True),
    StructField('prov_ord_edctn_cd', StringType(), True),
    StructField('prov_ord_edctn_cd_qual', StringType(), True),
    StructField('prov_ord_edctn_nm', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v7 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_prov_ord_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_prov_ord_id', StringType(), True),
    StructField('vdr_prov_ord_id_qual', StringType(), True),
    StructField('vdr_alt_prov_ord_id', StringType(), True),
    StructField('vdr_alt_prov_ord_id_qual', StringType(), True),
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
    StructField('prov_ord_dt', DateType(), True),
    StructField('ordg_prov_npi', StringType(), True),
    StructField('ordg_prov_vdr_id', StringType(), True),
    StructField('ordg_prov_vdr_id_qual', StringType(), True),
    StructField('ordg_prov_alt_id', StringType(), True),
    StructField('ordg_prov_alt_id_qual', StringType(), True),
    StructField('ordg_prov_tax_id', StringType(), True),
    StructField('ordg_prov_dea_id', StringType(), True),
    StructField('ordg_prov_state_lic_id', StringType(), True),
    StructField('ordg_prov_comrcl_id', StringType(), True),
    StructField('ordg_prov_upin', StringType(), True),
    StructField('ordg_prov_ssn', StringType(), True),
    StructField('ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('ordg_prov_alt_speclty_id', StringType(), True),
    StructField('ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('ordg_prov_fclty_nm', StringType(), True),
    StructField('ordg_prov_frst_nm', StringType(), True),
    StructField('ordg_prov_last_nm', StringType(), True),
    StructField('ordg_prov_addr_1_txt', StringType(), True),
    StructField('ordg_prov_addr_2_txt', StringType(), True),
    StructField('ordg_prov_state_cd', StringType(), True),
    StructField('ordg_prov_zip_cd', StringType(), True),
    StructField('ordg_fclty_npi', StringType(), True),
    StructField('ordg_fclty_vdr_id', StringType(), True),
    StructField('ordg_fclty_vdr_id_qual', StringType(), True),
    StructField('ordg_fclty_alt_id', StringType(), True),
    StructField('ordg_fclty_alt_id_qual', StringType(), True),
    StructField('ordg_fclty_tax_id', StringType(), True),
    StructField('ordg_fclty_state_lic_id', StringType(), True),
    StructField('ordg_fclty_comrcl_id', StringType(), True),
    StructField('ordg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('ordg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('ordg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('ordg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('ordg_fclty_alt_speclty_id', StringType(), True),
    StructField('ordg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('ordg_fclty_nm', StringType(), True),
    StructField('ordg_fclty_addr_1_txt', StringType(), True),
    StructField('ordg_fclty_addr_2_txt', StringType(), True),
    StructField('ordg_fclty_state_cd', StringType(), True),
    StructField('ordg_fclty_zip_cd', StringType(), True),
    StructField('prov_ord_start_dt', DateType(), True),
    StructField('prov_ord_end_dt', DateType(), True),
    StructField('prov_ord_ctgy_cd', StringType(), True),
    StructField('prov_ord_ctgy_cd_qual', StringType(), True),
    StructField('prov_ord_ctgy_nm', StringType(), True),
    StructField('prov_ord_ctgy_desc', StringType(), True),
    StructField('prov_ord_typ_cd', StringType(), True),
    StructField('prov_ord_typ_cd_qual', StringType(), True),
    StructField('prov_ord_typ_nm', StringType(), True),
    StructField('prov_ord_typ_desc', StringType(), True),
    StructField('prov_ord_cd', StringType(), True),
    StructField('prov_ord_cd_qual', StringType(), True),
    StructField('prov_ord_nm', StringType(), True),
    StructField('prov_ord_desc', StringType(), True),
    StructField('prov_ord_alt_cd', StringType(), True),
    StructField('prov_ord_alt_cd_qual', StringType(), True),
    StructField('prov_ord_alt_nm', StringType(), True),
    StructField('prov_ord_alt_desc', StringType(), True),
    StructField('prov_ord_ndc', StringType(), True),
    StructField('prov_ord_diag_cd', StringType(), True),
    StructField('prov_ord_diag_cd_qual', StringType(), True),
    StructField('prov_ord_diag_nm', StringType(), True),
    StructField('prov_ord_diag_desc', StringType(), True),
    StructField('prov_ord_snomed_cd', StringType(), True),
    StructField('prov_ord_vcx_cd', StringType(), True),
    StructField('prov_ord_vcx_cd_qual', StringType(), True),
    StructField('prov_ord_vcx_nm', StringType(), True),
    StructField('prov_ord_vcx_desc', StringType(), True),
    StructField('prov_ord_rsn_cd', StringType(), True),
    StructField('prov_ord_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_rsn_nm', StringType(), True),
    StructField('prov_ord_rsn_desc', StringType(), True),
    StructField('prov_ord_stat_cd', StringType(), True),
    StructField('prov_ord_stat_cd_qual', StringType(), True),
    StructField('prov_ord_stat_nm', StringType(), True),
    StructField('prov_ord_stat_desc', StringType(), True),
    StructField('prov_ord_complt_flg', StringType(), True),
    StructField('prov_ord_complt_dt', DateType(), True),
    StructField('prov_ord_durtn_day_cnt', IntegerType(), True),
    StructField('prov_ord_complt_rsn_cd', StringType(), True),
    StructField('prov_ord_complt_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_complt_rsn_nm', StringType(), True),
    StructField('prov_ord_complt_rsn_desc', StringType(), True),
    StructField('prov_ord_cxld_rsn_cd', StringType(), True),
    StructField('prov_ord_cxld_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_cxld_rsn_nm', StringType(), True),
    StructField('prov_ord_cxld_rsn_desc', StringType(), True),
    StructField('prov_ord_cxld_dt', DateType(), True),
    StructField('prov_ord_cxld_flg', StringType(), True),
    StructField('prov_ord_result_cd', StringType(), True),
    StructField('prov_ord_result_cd_qual', StringType(), True),
    StructField('prov_ord_result_nm', StringType(), True),
    StructField('prov_ord_result_desc', StringType(), True),
    StructField('prov_ord_result_rcvd_dt', DateType(), True),
    StructField('prov_ord_trtmt_typ_cd', StringType(), True),
    StructField('prov_ord_trtmt_typ_cd_qual', StringType(), True),
    StructField('prov_ord_trtmt_typ_nm', StringType(), True),
    StructField('prov_ord_trtmt_typ_desc', StringType(), True),
    StructField('prov_ord_rfrd_speclty_cd', StringType(), True),
    StructField('prov_ord_rfrd_speclty_cd_qual', StringType(), True),
    StructField('prov_ord_rfrd_speclty_nm', StringType(), True),
    StructField('prov_ord_rfrd_speclty_desc', StringType(), True),
    StructField('prov_ord_specl_instrs_cd', StringType(), True),
    StructField('prov_ord_specl_instrs_cd_qual', StringType(), True),
    StructField('prov_ord_specl_instrs_nm', StringType(), True),
    StructField('prov_ord_specl_instrs_desc', StringType(), True),
    StructField('prov_ord_edctn_flg', StringType(), True),
    StructField('prov_ord_edctn_dt', DateType(), True),
    StructField('prov_ord_edctn_cd', StringType(), True),
    StructField('prov_ord_edctn_cd_qual', StringType(), True),
    StructField('prov_ord_edctn_nm', StringType(), True),
    StructField('prov_ord_edctn_desc', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v9 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_prov_ord_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_prov_ord_id', StringType(), True),
    StructField('vdr_prov_ord_id_qual', StringType(), True),
    StructField('vdr_alt_prov_ord_id', StringType(), True),
    StructField('vdr_alt_prov_ord_id_qual', StringType(), True),
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
    StructField('prov_ord_dt', DateType(), True),
    StructField('prov_ord_prov_npi', StringType(), True),
    StructField('prov_ord_prov_qual', StringType(), True),
    StructField('prov_ord_prov_vdr_id', StringType(), True),
    StructField('prov_ord_prov_vdr_id_qual', StringType(), True),
    StructField('prov_ord_prov_alt_id', StringType(), True),
    StructField('prov_ord_prov_alt_id_qual', StringType(), True),
    StructField('prov_ord_prov_tax_id', StringType(), True),
    StructField('prov_ord_prov_state_lic_id', StringType(), True),
    StructField('prov_ord_prov_comrcl_id', StringType(), True),
    StructField('prov_ord_prov_upin', StringType(), True),
    StructField('prov_ord_prov_ssn', StringType(), True),
    StructField('prov_ord_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('prov_ord_prov_alt_taxnmy_id', StringType(), True),
    StructField('prov_ord_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('prov_ord_prov_mdcr_speclty_cd', StringType(), True),
    StructField('prov_ord_prov_alt_speclty_id', StringType(), True),
    StructField('prov_ord_prov_alt_speclty_id_qual', StringType(), True),
    StructField('prov_ord_prov_frst_nm', StringType(), True),
    StructField('prov_ord_prov_last_nm', StringType(), True),
    StructField('prov_ord_prov_fclty_nm', StringType(), True),
    StructField('prov_ord_prov_addr_1_txt', StringType(), True),
    StructField('prov_ord_prov_addr_2_txt', StringType(), True),
    StructField('prov_ord_prov_state_cd', StringType(), True),
    StructField('prov_ord_prov_zip_cd', StringType(), True),
    StructField('ordg_prov_npi', StringType(), True),
    StructField('ordg_prov_vdr_id', StringType(), True),
    StructField('ordg_prov_vdr_id_qual', StringType(), True),
    StructField('ordg_prov_alt_id', StringType(), True),
    StructField('ordg_prov_alt_id_qual', StringType(), True),
    StructField('ordg_prov_tax_id', StringType(), True),
    StructField('ordg_prov_state_lic_id', StringType(), True),
    StructField('ordg_prov_comrcl_id', StringType(), True),
    StructField('ordg_prov_upin', StringType(), True),
    StructField('ordg_prov_ssn', StringType(), True),
    StructField('ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('ordg_prov_alt_speclty_id', StringType(), True),
    StructField('ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('ordg_prov_fclty_nm', StringType(), True),
    StructField('ordg_prov_frst_nm', StringType(), True),
    StructField('ordg_prov_last_nm', StringType(), True),
    StructField('ordg_prov_addr_1_txt', StringType(), True),
    StructField('ordg_prov_addr_2_txt', StringType(), True),
    StructField('ordg_prov_state_cd', StringType(), True),
    StructField('ordg_prov_zip_cd', StringType(), True),
    StructField('ordg_fclty_npi', StringType(), True),
    StructField('ordg_fclty_vdr_id', StringType(), True),
    StructField('ordg_fclty_vdr_id_qual', StringType(), True),
    StructField('ordg_fclty_alt_id', StringType(), True),
    StructField('ordg_fclty_alt_id_qual', StringType(), True),
    StructField('ordg_fclty_tax_id', StringType(), True),
    StructField('ordg_fclty_state_lic_id', StringType(), True),
    StructField('ordg_fclty_comrcl_id', StringType(), True),
    StructField('ordg_fclty_nucc_taxnmy_cd', StringType(), True),
    StructField('ordg_fclty_alt_taxnmy_id', StringType(), True),
    StructField('ordg_fclty_alt_taxnmy_id_qual', StringType(), True),
    StructField('ordg_fclty_mdcr_speclty_cd', StringType(), True),
    StructField('ordg_fclty_alt_speclty_id', StringType(), True),
    StructField('ordg_fclty_alt_speclty_id_qual', StringType(), True),
    StructField('ordg_fclty_nm', StringType(), True),
    StructField('ordg_fclty_addr_1_txt', StringType(), True),
    StructField('ordg_fclty_addr_2_txt', StringType(), True),
    StructField('ordg_fclty_state_cd', StringType(), True),
    StructField('ordg_fclty_zip_cd', StringType(), True),
    StructField('prov_ord_start_dt', DateType(), True),
    StructField('prov_ord_end_dt', DateType(), True),
    StructField('prov_ord_ctgy_cd', StringType(), True),
    StructField('prov_ord_ctgy_cd_qual', StringType(), True),
    StructField('prov_ord_ctgy_nm', StringType(), True),
    StructField('prov_ord_ctgy_desc', StringType(), True),
    StructField('prov_ord_typ_cd', StringType(), True),
    StructField('prov_ord_typ_cd_qual', StringType(), True),
    StructField('prov_ord_typ_nm', StringType(), True),
    StructField('prov_ord_typ_desc', StringType(), True),
    StructField('prov_ord_cd', StringType(), True),
    StructField('prov_ord_cd_qual', StringType(), True),
    StructField('prov_ord_nm', StringType(), True),
    StructField('prov_ord_desc', StringType(), True),
    StructField('prov_ord_alt_cd', StringType(), True),
    StructField('prov_ord_alt_cd_qual', StringType(), True),
    StructField('prov_ord_alt_nm', StringType(), True),
    StructField('prov_ord_alt_desc', StringType(), True),
    StructField('prov_ord_ndc', StringType(), True),
    StructField('prov_ord_diag_cd', StringType(), True),
    StructField('prov_ord_diag_cd_qual', StringType(), True),
    StructField('prov_ord_diag_nm', StringType(), True),
    StructField('prov_ord_snomed_cd', StringType(), True),
    StructField('prov_ord_vcx_cd', StringType(), True),
    StructField('prov_ord_vcx_cd_qual', StringType(), True),
    StructField('prov_ord_vcx_nm', StringType(), True),
    StructField('prov_ord_rsn_cd', StringType(), True),
    StructField('prov_ord_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_rsn_nm', StringType(), True),
    StructField('prov_ord_rsn_desc', StringType(), True),
    StructField('prov_ord_stat_cd', StringType(), True),
    StructField('prov_ord_stat_dt', DateType(), True),
    StructField('prov_ord_stat_rsn_cd', StringType(), True),
    StructField('prov_ord_stat_cd_qual', StringType(), True),
    StructField('prov_ord_stat_nm', StringType(), True),
    StructField('prov_ord_stat_desc', StringType(), True),
    StructField('prov_ord_complt_flg', StringType(), True),
    StructField('prov_ord_complt_dt', DateType(), True),
    StructField('prov_ord_durtn_day_cnt', IntegerType(), True),
    StructField('prov_ord_complt_rsn_cd', StringType(), True),
    StructField('prov_ord_complt_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_complt_rsn_nm', StringType(), True),
    StructField('prov_ord_complt_rsn_desc', StringType(), True),
    StructField('prov_ord_cxld_rsn_cd', StringType(), True),
    StructField('prov_ord_cxld_rsn_cd_qual', StringType(), True),
    StructField('prov_ord_cxld_rsn_nm', StringType(), True),
    StructField('prov_ord_cxld_rsn_desc', StringType(), True),
    StructField('prov_ord_cxld_dt', DateType(), True),
    StructField('prov_ord_cxld_flg', StringType(), True),
    StructField('prov_ord_result_cd', StringType(), True),
    StructField('prov_ord_result_cd_qual', StringType(), True),
    StructField('prov_ord_result_nm', StringType(), True),
    StructField('prov_ord_result_desc', StringType(), True),
    StructField('prov_ord_result_rcvd_dt', DateType(), True),
    StructField('prov_ord_trtmt_typ_cd', StringType(), True),
    StructField('prov_ord_trtmt_typ_cd_qual', StringType(), True),
    StructField('prov_ord_trtmt_typ_nm', StringType(), True),
    StructField('prov_ord_rfrd_speclty_cd', StringType(), True),
    StructField('prov_ord_rfrd_speclty_cd_qual', StringType(), True),
    StructField('prov_ord_rfrd_speclty_nm', StringType(), True),
    StructField('prov_ord_rfrd_speclty_desc', StringType(), True),
    StructField('prov_ord_specl_instrs_cd', StringType(), True),
    StructField('prov_ord_specl_instrs_cd_qual', StringType(), True),
    StructField('prov_ord_specl_instrs_nm', StringType(), True),
    StructField('prov_ord_specl_instrs_desc', StringType(), True),
    StructField('prov_ord_edctn_flg', StringType(), True),
    StructField('prov_ord_edctn_dt', DateType(), True),
    StructField('prov_ord_edctn_cd', StringType(), True),
    StructField('prov_ord_edctn_cd_qual', StringType(), True),
    StructField('prov_ord_edctn_nm', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])


data_type = DataType.EMR
schemas = {
    'schema_v4': Schema(name='schema_v4',
                        schema_structure=schema_v4,
                        distribution_key='row_id',
                        data_type=data_type,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=DataType(data_type).value + '/2017-08-23/provider_order'
                        ),
    'schema_v6': Schema(name='schema_v6',
                        schema_structure=schema_v6,
                        distribution_key='row_id',
                        data_type=data_type,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=DataType(data_type).value + '/2017-08-23/provider_order'
                        ),
    'schema_v7': Schema(name='schema_v7',
                        schema_structure=schema_v7,
                        distribution_key='row_id',
                        data_type=data_type,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=DataType(data_type).value + '/2017-08-23/provider_order'
                        ),
    'schema_v9': Schema(name='schema_v9',
                        schema_structure=schema_v9,
                        distribution_key='row_id',
                        data_type=data_type,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=DataType(data_type).value + '/2017-08-23/provider_order'
                        )
}
