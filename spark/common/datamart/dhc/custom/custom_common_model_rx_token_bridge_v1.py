"""
dhc Custom v1 Rx Token Bridge schema
"""
# pylint: disable=duplicate-code
from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='dhc/custom_mart/2021-07-01/rx_token_bridge/',
    distribution_key='concat_unique_fields',
    provider_partition_column='part_provider',
    date_partition_column='part_best_date',
    schema_structure=StructType([
        StructField('claim_id', StringType(), True),
        StructField('concat_unique_fields', StringType(), True),
        StructField('datavant_token1', StringType(), True),
        StructField('datavant_token2', StringType(), True),
        StructField('hvid', StringType(), True),
        StructField('date_service', DateType(), True),
        StructField('rx_number', StringType(), True),
        StructField('cob_count', StringType(), True),
        StructField('pharmacy_other_id', StringType(), True),
        StructField('response_code_vendor', StringType(), True),
        StructField('ndc_code', StringType(), True),
        StructField('created', DateType(), True),
        StructField('data_vendor', StringType(), True)
    ])
)


