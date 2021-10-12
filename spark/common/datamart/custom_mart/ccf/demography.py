"""
demography schema
"""

from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='ccf/2021-08-31/demography/',
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
        StructField('ethnicity', StringType(), True),
        StructField('race', StringType(), True),
        StructField('religion', StringType(), True),
        StructField('organ_donor_flag', StringType(), True),
        StructField('date_of_consent', StringType(), True),
        StructField('date_of_consent_withdrawn', StringType(), True),
        StructField('deceased_death_flag', StringType(), True),
        StructField('encounter_date', DateType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
    ])
)
