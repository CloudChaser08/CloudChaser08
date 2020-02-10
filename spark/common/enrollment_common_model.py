from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

schema_v5 = StructType([
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


data_type = DataType.ENROLLMENT_RECORDS
output_directory = DataType(data_type).value + '/2017-03-22'
schemas = {
    'schema_v5': Schema(name='schema_v5',
                        schema_structure=schema_v5,
                        data_type=DataType.ENROLLMENT_RECORDS,
                        provider_partition_column='part_provider',
                        date_partition_column='part_best_date',
                        output_directory=output_directory)
}
