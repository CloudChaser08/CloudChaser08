from pyspark.sql.types import *
from spark.common.schema import Schema

schema_v1 = StructType([
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
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('enc_start_dt', DateType(), True),
    StructField('enc_end_dt', DateType(), True),
    StructField('enc_fclty_npi', StringType(), True),
    StructField('enc_fclty_id', StringType(), True),
    StructField('enc_fclty_id_qual', StringType(), True),
    StructField('enc_fclty_state_cd', StringType(), True),
    StructField('enc_fclty_zip_cd', StringType(), True),
    StructField('enc_fclty_geo_txt', StringType(), True),
    StructField('enc_fclty_grp_txt', StringType(), True),
    StructField('enc_prov_id', StringType(), True),
    StructField('enc_prov_id_qual', StringType(), True),
    StructField('enc_admtg_prov_npi', StringType(), True),
    StructField('enc_admtg_prov_id', StringType(), True),
    StructField('enc_admtg_prov_id_qual', StringType(), True),
    StructField('enc_rndrg_prov_npi', StringType(), True),
    StructField('enc_rndrg_prov_id', StringType(), True),
    StructField('enc_rndrg_prov_id_qual', StringType(), True),
    StructField('enc_operg_prov_npi', StringType(), True),
    StructField('enc_operg_prov_id', StringType(), True),
    StructField('enc_operg_prov_id_qual', StringType(), True),
    StructField('enc_svg_prov_npi', StringType(), True),
    StructField('enc_svg_prov_id', StringType(), True),
    StructField('enc_svg_prov_id_qual', StringType(), True),
    StructField('enc_rfrg_prov_npi', StringType(), True),
    StructField('enc_rfrg_prov_id', StringType(), True),
    StructField('enc_rfrg_prov_id_qual', StringType(), True),
    StructField('vdr_ptnt_id', StringType(), True),
    StructField('enc_grp_txt', StringType(), True),
    StructField('los_day_cnt', StringType(), True),
    StructField('los_txt', StringType(), True),
    StructField('admsn_src_std_cd', StringType(), True),
    StructField('admsn_src_vdr_cd', StringType(), True),
    StructField('admsn_src_vdr_cd_qual', StringType(), True),
    StructField('admsn_typ_std_cd', StringType(), True),
    StructField('admsn_typ_vdr_cd', StringType(), True),
    StructField('admsn_typ_vdr_cd_qual', StringType(), True),
    StructField('dischg_stat_std_cd', StringType(), True),
    StructField('dischg_stat_vdr_cd', StringType(), True),
    StructField('dischg_stat_vdr_cd_qual', StringType(), True),
    StructField('bill_typ_std_cd', StringType(), True),
    StructField('bill_typ_vdr_cd', StringType(), True),
    StructField('bill_typ_vdr_cd_qual', StringType(), True),
    StructField('drg_cd', StringType(), True),
    StructField('drg_cd_qual', StringType(), True),
    StructField('tot_chg_amt', FloatType(), True),
    StructField('tot_actl_pymt_amt', FloatType(), True),
    StructField('tot_expctd_pymt_amt', FloatType(), True),
    StructField('tot_disc_amt', FloatType(), True),
    StructField('tot_ptnt_liabty_amt', FloatType(), True),
    StructField('tot_ptnt_pymt_amt', FloatType(), True),
    StructField('tot_bal_amt', FloatType(), True),
    StructField('tot_eob_wrtoff_amt', FloatType(), True),
    StructField('tot_cntrctl_adjmt_amt', FloatType(), True),
    StructField('tot_other_adjmt_amt', FloatType(), True),
    StructField('tot_other_pymt_amt', FloatType(), True),
    StructField('tot_denied_amt', FloatType(), True),
    StructField('tot_ptnt_cpy_amt', FloatType(), True),
    StructField('tot_ptnt_ddctbl_amt', FloatType(), True),
    StructField('tot_secdy_payr_pymt_amt', FloatType(), True),
    StructField('tot_recalcd_cost_amt', FloatType(), True),
    StructField('tot_recalcd_cost_amt_qual', StringType(), True),
    StructField('payr_grp_txt', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

output_folder = '2019-08-15'
schemas = {
    'schema_v1': Schema(name='schema_v1',
                        schema_structure=schema_v1,
                        output_folder=output_folder,
                        distribution_key='row_id',
                        data_type='cdm',
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        staging_subdir='encounter')
}
