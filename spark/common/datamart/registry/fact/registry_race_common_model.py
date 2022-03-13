"""
registry medicalclaims race schema
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
        StructField('race', StringType(), True),
        StructField('patient_year_of_birth', StringType(), True),
        StructField('stg_file_date', DateType(), True)
    ])

data_type = DataType.MEDICAL_CLAIMS
schemas = {
    'schema_v1': Schema(name='schema_v1',
                        schema_structure=schema_v1,
                        distribution_key='record_id',
                        data_type=DataType(data_type).value,
                        provider_partition_column='part_provider',
                        date_partition_column='part_best_date',
                        output_directory='registry/race/2021-11-21/'
                        )
}