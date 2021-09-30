"""consumer v10 model"""
from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v10',
    data_type='consumer',
    output_directory='consumer/2017-08-02',
    distribution_key='record_id',
    date_partition_column='part_best_date',
    provider_partition_column='part_provider',
    schema_structure=StructType([
        StructField('record_id', LongType(), True),
        StructField('hvid', StringType(), True),
        StructField('created', DateType(), True),
        StructField('model_version', StringType(), True),
        StructField('data_set', StringType(), True),
        StructField('data_feed', StringType(), True),
        StructField('data_vendor', StringType(), True),
        StructField('source_version', StringType(), True),
        StructField('patient_age', StringType(), True),
        StructField('patient_year_of_birth', StringType(), True),
        StructField('patient_zip', StringType(), True),
        StructField('patient_zip3', StringType(), True),
        StructField('patient_state', StringType(), True),
        StructField('patient_gender', StringType(), True),
        StructField('source_record_id', StringType(), True),
        StructField('source_record_qual', StringType(), True),
        StructField('source_record_date', DateType(), True),
        StructField('patient_group', StringType(), True),
        StructField('event', StringType(), True),
        StructField('event_val', StringType(), True),
        StructField('event_val_uom', StringType(), True),
        StructField('event_units', StringType(), True),
        StructField('event_units_uom', StringType(), True),
        StructField('event_date', DateType(), True),
        StructField('event_date_qual', StringType(), True),
        StructField('event_zip', StringType(), True),
        StructField('event_revenue', StringType(), True),
        StructField('event_category_code', StringType(), True),
        StructField('event_category_code_qual', StringType(), True),
        StructField('event_category_name', StringType(), True),
        StructField('event_category_flag', StringType(), True),
        StructField('event_category_flag_qual', StringType(), True),
        StructField('logical_delete_reason', StringType(), True)
])
)