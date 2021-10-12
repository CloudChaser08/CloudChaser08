"""
clinical observation emr schema
"""

from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='ccf/2021-08-31/clinical_observation_crf/',
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
        StructField('obs_id', StringType(), True),
        StructField('visit_encounter_id', StringType(), True),
        StructField('obs_test_concept_name', StringType(), True),
        StructField('obs_test_concept_code', StringType(), True),
        StructField('obs_test_system_name', StringType(), True),
        StructField('src_obs_test_concept_code', StringType(), True),
        StructField('src_obs_test_system_name', StringType(), True),
        StructField('src_obs_test_system_desc', StringType(), True),
        StructField('test_result_numeric', StringType(), True),
        StructField('obs_test_result_date', StringType(), True),
        StructField('descriptive_symp_test_results', StringType(), True),
        StructField('obs_test_ty_concept_code', StringType(), True),
        StructField('obs_test_ty_concept_name', StringType(), True),
        StructField('ana_site_concept_name', StringType(), True),
        StructField('ana_site_concept_code', StringType(), True),
        StructField('obs_event_start_date', StringType(), True),
        StructField('obs_event_end_date', StringType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
    ])
)
