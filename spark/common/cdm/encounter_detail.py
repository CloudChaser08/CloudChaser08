"""cdm encounter detail."""
from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

schema_v1 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_enc_dtl_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_enc_dtl_id', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_start_dt', DateType(), True),
    StructField('enc_end_dt', DateType(), True),
    StructField('proc_dt', DateType(), True),
    StructField('chg_dt', DateType(), True),
    StructField('proc_cd', StringType(), True),
    StructField('proc_cd_qual', StringType(), True),
    StructField('proc_cd_1_modfr', StringType(), True),
    StructField('proc_cd_2_modfr', StringType(), True),
    StructField('proc_cd_3_modfr', StringType(), True),
    StructField('proc_cd_4_modfr', StringType(), True),
    StructField('proc_seq_cd', StringType(), True),
    StructField('proc_ndc', StringType(), True),
    StructField('proc_unit_qty', StringType(), True),
    StructField('proc_uom', StringType(), True),
    StructField('proc_grp_txt', StringType(), True),
    StructField('medctn_ndc', StringType(), True),
    StructField('medctn_molcl_nm', StringType(), True),
    StructField('medctn_qty', StringType(), True),
    StructField('dtl_chg_amt', FloatType(), True),
    StructField('chg_meth_desc', StringType(), True),
    StructField('cdm_grp_txt', StringType(), True),
    StructField('cdm_convsn_txt', StringType(), True),
    StructField('cdm_dept_txt', StringType(), True),
    StructField('std_cdm_grp_txt', StringType(), True),
    StructField('vdr_chg_desc', StringType(), True),
    StructField('std_chg_desc', StringType(), True),
    StructField('cdm_manfctr_txt', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v2 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_enc_dtl_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_enc_dtl_id', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_start_dt', DateType(), True),
    StructField('enc_end_dt', DateType(), True),
    StructField('proc_dt', DateType(), True),
    StructField('chg_dt', DateType(), True),
    StructField('medctn_admin_dt', DateType(), True),
    StructField('proc_cd', StringType(), True),
    StructField('proc_cd_qual', StringType(), True),
    StructField('proc_cd_1_modfr', StringType(), True),
    StructField('proc_cd_2_modfr', StringType(), True),
    StructField('proc_cd_3_modfr', StringType(), True),
    StructField('proc_cd_4_modfr', StringType(), True),
    StructField('proc_seq_cd', StringType(), True),
    StructField('proc_ndc', StringType(), True),
    StructField('proc_unit_qty', StringType(), True),
    StructField('proc_uom', StringType(), True),
    StructField('proc_grp_txt', StringType(), True),
    StructField('medctn_ndc', StringType(), True),
    StructField('medctn_molcl_nm', StringType(), True),
    StructField('medctn_qty', StringType(), True),
    StructField('dtl_chg_amt', FloatType(), True),
    StructField('chg_meth_desc', FloatType(), True),    # WARNING: Does not match documentation.
    StructField('cdm_grp_txt', StringType(), True),
    StructField('cdm_convsn_txt', StringType(), True),
    StructField('cdm_dept_txt', StringType(), True),
    StructField('std_cdm_grp_txt', StringType(), True),
    StructField('vdr_chg_desc', StringType(), True),
    StructField('std_chg_desc', StringType(), True),
    StructField('cdm_manfctr_txt', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v4 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_enc_dtl_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_enc_dtl_id', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_start_dt', DateType(), True),
    StructField('enc_end_dt', DateType(), True),
    StructField('proc_dt', DateType(), True),
    StructField('chg_dt', DateType(), True),
    StructField('medctn_admin_dt', DateType(), True),
    StructField('proc_cd', StringType(), True),
    StructField('proc_cd_qual', StringType(), True),
    StructField('proc_cd_1_modfr', StringType(), True),
    StructField('proc_cd_2_modfr', StringType(), True),
    StructField('proc_cd_3_modfr', StringType(), True),
    StructField('proc_cd_4_modfr', StringType(), True),
    StructField('proc_seq_cd', StringType(), True),
    StructField('proc_ndc', StringType(), True),
    StructField('proc_unit_qty', StringType(), True),
    StructField('proc_uom', StringType(), True),
    StructField('proc_grp_txt', StringType(), True),
    StructField('medctn_ndc', StringType(), True),
    StructField('medctn_molcl_nm', StringType(), True),
    StructField('medctn_qty', StringType(), True),
    StructField('dtl_chg_amt', FloatType(), True),
    StructField('chg_meth_desc', StringType(), True),
    StructField('cdm_grp_txt', StringType(), True),
    StructField('cdm_convsn_txt', StringType(), True),
    StructField('cdm_dept_txt', StringType(), True),
    StructField('std_cdm_grp_txt', StringType(), True),
    StructField('vdr_chg_desc', StringType(), True),
    StructField('std_chg_desc', StringType(), True),
    StructField('cdm_manfctr_txt', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True),
    StructField('icu_ccu_flg', StringType(), True)
])

data_type = DataType.CDM
output_directory = DataType(data_type).value + '/2019-08-15/encounter_detail'

schemas = {
    'schema_v1': Schema(name='schema_v1',
                        schema_structure=schema_v1,
                        distribution_key='row_id',
                        data_type=data_type.value,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=output_directory),

    'schema_v2': Schema(name='schema_v2',
                        schema_structure=schema_v2,
                        distribution_key='row_id',
                        data_type=data_type.value,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=output_directory),

    'schema_v4': Schema(name='schema_v4',
                        schema_structure=schema_v4,
                        distribution_key='row_id',
                        data_type=data_type.value,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=output_directory)
}
