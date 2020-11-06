"""
Specification for the schema_v5 enrollment_common_model
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType
from spark.common.schema import Schema


schema = Schema(
    name='schema_v5',
    data_type="enrollmentrecords",
    output_directory='enrollmentrecords/2017-03-22',
    provider_partition_column='part_provider',
    date_partition_column='part_best_date',
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
        StructField('patient_zip3', StringType(), True),
        StructField('patient_state', StringType(), True),
        StructField('patient_gender', StringType(), True),
        StructField('source_record_id', StringType(), True),
        StructField('source_record_qual', StringType(), True),
        StructField('source_record_date', DateType(), True),
        StructField('date_start', DateType(), True),
        StructField('date_end', DateType(), True),
        StructField('date_enrolled', DateType(), True),
        StructField('benefit_type', StringType(), True),
        StructField('payer_plan_name', StringType(), True),
        StructField('payer_type', StringType(), True)
    ])
)
