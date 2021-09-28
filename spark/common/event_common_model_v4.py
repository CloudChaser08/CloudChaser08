"""event common model v4"""
from pyspark.sql.types import *

schema = StructType([
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
    StructField('event', StringType(), True),
    StructField('event_val', StringType(), True),
    StructField('event_val_uom', StringType(), True),
    StructField('event_date', DateType(), True)
])
