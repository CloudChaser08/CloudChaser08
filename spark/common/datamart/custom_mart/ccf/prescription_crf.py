"""
perscreption crf schema
"""

from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='ccf/2021-08-31/prescription_crf/',
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
        StructField('med_id', StringType(), True),
        StructField('visit_encounter_id', StringType(), True),
        StructField('medication_name', StringType(), True),
        StructField('drug_code', StringType(), True),
        StructField('drug_code_sys_nm', StringType(), True),
        StructField('src_drug_code', StringType(), True),
        StructField('src_drug_code_sys_nm', StringType(), True),
        StructField('src_drug_code_concept_name', StringType(), True),
        StructField('med_action_concept_name', StringType(), True),
        StructField('route_of_medication', StringType(), True),
        StructField('medication_domain', StringType(), True),
        StructField('med_start_date', StringType(), True),
        StructField('med_end_date', StringType(), True),
        StructField('dose_of_medication', StringType(), True),
        StructField('current_medication', StringType(), True),
        StructField('other_medication', StringType(), True),
        StructField('unit_of_measure_for_medication', StringType(), True),
        StructField('medication_frequence', StringType(), True),
        StructField('medication_administrated_code', StringType(), True),
        StructField('medication_administrated', StringType(), True),
        StructField('frequency_in_days', StringType(), True),
        StructField('reason_stopped', StringType(), True),
        StructField('summary', StringType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
    ])
)
