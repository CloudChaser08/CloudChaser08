from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

schema_v1 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_diag_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_diag_id', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_start_dt', DateType(), True),
    StructField('enc_end_dt', DateType(), True),
    StructField('diag_cd', StringType(), True),
    StructField('diag_cd_qual', StringType(), True),
    StructField('diag_prty_cd', StringType(), True),
    StructField('admtg_diag_flg', StringType(), True),
    StructField('prmy_diag_flg', StringType(), True),
    StructField('diag_grp_txt', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

data_type = DataType.CDM
output_directory = DataType(data_type).value + '/2019-08-15/diagnosis'
schemas = {
    'schema_v1': Schema(name='schema_v1',
                        schema_structure=schema_v1,
                        distribution_key='row_id',
                        data_type=DataType.CDM,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=output_directory)
}
