"""
lab test emrschema
"""

from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='ccf/2021-08-31/lab_test_emr/',
    distribution_key='record_id',
    provider_partition_column='part_provider',
    date_partition_column='part_mth',
    schema_structure=StructType([
        StructField('record_id', LongType(), True),
        StructField('crt_dt', DateType(), True),
        StructField('data_set_nm', StringType(), True),
        StructField('hvid', StringType(), True),
        StructField('ptnt_birth_yr', IntegerType(), True),
        StructField('ptnt_age_num', StringType(), True),
        StructField('ptnt_gender_cd', StringType(), True),
        StructField('ptnt_state_cd', StringType(), True),
        StructField('ptnt_zip3_cd', StringType(), True),
        StructField('deidentified_master_patient_id', StringType(), True),
        StructField('deidentified_patient_id', StringType(), True),
        StructField('data_source', StringType(), True),
        StructField('lab_id', StringType(), True),
        StructField('visit_encounter_id', StringType(), True),
        StructField('lab_test_concept_name', StringType(), True),
        StructField('lab_test_concept_code', StringType(), True),
        StructField('lab_test_system_name', StringType(), True),
        StructField('src_lab_test_concept_code', StringType(), True),
        StructField('src_lab_test_system_name', StringType(), True),
        StructField('src_lab_test_concept_name', StringType(), True),
        StructField('lab_test_sts_concept_code', StringType(), True),
        StructField('lab_test_sts_concept_name', StringType(), True),
        StructField('lab_results', StringType(), True),
        StructField('test_unit', StringType(), True),
        StructField('test_result_numeric', StringType(), True),
        StructField('specimen_collection_date', StringType(), True),
        StructField('lab_test_requested_date', StringType(), True),
        StructField('lab_test_result_date', StringType(), True),
        StructField('abnormal_flag', StringType(), True),
        StructField('normal_max', StringType(), True),
        StructField('normal_min', StringType(), True),
        StructField('prmy_src_tbl_nm',StringType(), True)
    ])
)
