"""
dhc medicalclaims v1 serviceline schema
"""
# pylint: disable=duplicate-code
from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='medicalclaims',
    output_directory='dhc/medicalclaims/2021-05-01/service_lines/',
    distribution_key='claimid',
    provider_partition_column='part_provider',
    date_partition_column='part_best_date',
    schema_structure=StructType([
        StructField('claimid', StringType(), True),
        StructField('recordtype', StringType(), True),
        StructField('linenumber', IntegerType(), True),
        StructField('servicefromdate', DateType(), True),
        StructField('servicetodate', DateType(), True),
        StructField('placeofserviceid', StringType(), True),
        StructField('procedurecode', StringType(), True),
        StructField('procedurecodequal', StringType(), True),
        StructField('proceduremodifier1', StringType(), True),
        StructField('proceduremodifier2', StringType(), True),
        StructField('proceduremodifier3', StringType(), True),
        StructField('proceduremodifier4', StringType(), True),
        StructField('linecharges', FloatType(), True),
        StructField('unitcount', FloatType(), True),
        StructField('revenuecode', StringType(), True),
        StructField('diagnosiscodepointer1', StringType(), True),
        StructField('diagnosiscodepointer2', StringType(), True),
        StructField('diagnosiscodepointer3', StringType(), True),
        StructField('diagnosiscodepointer4', StringType(), True),
        StructField('ndccode', StringType(), True),
        StructField('emergencyind', StringType(), True),
        StructField('sourcefilename', StringType(), True),
        StructField('data_vendor', StringType(), True),
        StructField('dhcreceiveddate', StringType(), True),
        StructField('rownumber', LongType(), True),
        StructField('fileyear', IntegerType(), True),
        StructField('filemonth', IntegerType(), True),
        StructField('fileday', IntegerType(), True)
    ])
)
