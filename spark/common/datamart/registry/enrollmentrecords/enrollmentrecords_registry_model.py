"""
registry enrollmentrecords claims schema
"""
# pylint: disable=duplicate-code
from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

schema_v1 = StructType([
    StructField('record_id', LongType(), True),
    StructField('registry_type', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('created', DateType(), True),
    StructField('model_version', StringType(), True),
    StructField('data_set', StringType(), True),
    StructField('data_feed', StringType(), True),
    StructField('data_vendor', StringType(), True),
    StructField('patient_gender', StringType(), True),
    StructField('source_record_date', DateType(), True),
    StructField('date_start', DateType(), True),
    StructField('date_end', DateType(), True),
    StructField('benefit_type', StringType(), True),
    StructField('payer_type', StringType(), True),
    StructField('payer_grp_txt', StringType(), True),
    StructField('stg_file_date', DateType(), True)
])

data_type = DataType.ENROLLMENT_RECORDS
schemas = {
    'schema_v1': Schema(name='schema_v1',
                        schema_structure=schema_v1,
                        distribution_key='record_id',
                        data_type=DataType(data_type).value,
                        provider_partition_column='part_provider',
                        date_partition_column='part_best_date',
                        output_directory='registry/' + DataType(data_type).value + '/2021-11-21/'
                        )
}
