from pyspark.sql.types import *
from spark.common.schema import Schema

schema_v1 = StructType([
    StructField('hvid', StringType(), True),
    StructField('claimID', StringType(), True),
    StructField('test3', StringType(), True),
    StructField('test4', StringType(), True),
    StructField('test5', StringType(), True)
])

output_folder = '2019-08-15'
schemas = {
    'schema_v1': Schema(name='schema_v1',
                        schema_structure=schema_v1,
                        output_folder=output_folder,
                        distribution_key='row_id',
                        data_type='TEST_CLAIMS',
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        staging_subdir='test')
}
