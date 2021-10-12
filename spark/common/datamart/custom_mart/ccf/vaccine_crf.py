"""
vaccine crf schema
"""

from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='ccf/2021-08-31/vaccine_crf/',
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
        StructField('vaccination_id', StringType(), True),
        StructField('visit_encounter_id', StringType(), True),
        StructField('vacc_concept_name', StringType(), True),
        StructField('vacc_concept_code', StringType(), True),
        StructField('vacc_system_name', StringType(), True),
        StructField('vaccine_name', StringType(), True),
        StructField('vaccine_date', StringType(), True),
        StructField('route_of_administration', StringType(), True),
        StructField('vaccine_site', StringType(), True),
        StructField('immunization_active', StringType(), True),
        StructField('manufacturer', StringType(), True),
        StructField('lot_number', StringType(), True),
        StructField('vaccine_info_stmt_provided', StringType(), True),
        StructField('place_of_service', StringType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
    ])
)
