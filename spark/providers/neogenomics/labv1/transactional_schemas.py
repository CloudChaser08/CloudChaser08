"""
Transactional schema
"""
from pyspark.sql.types import StringType, StructType, StructField

tests_schema = StructType([
    StructField('hdr_rec_typ_cde', StringType(), True),
    StructField('patient_id', StringType(), True),
    StructField('test_order_id', StringType(), True),
    StructField('status', StringType(), True),
    StructField('level_of_service', StringType(), True),
    StructField('technology', StringType(), True),
    StructField('specimen_type', StringType(), True),
    StructField('body_site', StringType(), True),
    StructField('icd_code', StringType(), True),
    StructField('specimen_collected_date', StringType(), True),
    StructField('test_ordered_date', StringType(), True),
    StructField('test_reported_date', StringType(), True),
    StructField('test_canceled_date', StringType(), True),
    StructField('panel_name', StringType(), True),
    StructField('panel_code', StringType(), True),
    StructField('test_name', StringType(), True),
    StructField('test_code', StringType(), True),
    StructField('client_zip', StringType(), True)
])

results_schema = StructType([
    StructField('hdr_rec_typ_cde', StringType(), True),
    StructField('test_order_id', StringType(), True),
    StructField('result_name', StringType(), True),
    StructField('result_value', StringType(), True)
])
