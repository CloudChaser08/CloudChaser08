#!/usr/bin/env python2.7
"""
Create a compressed version of LOINC to be loaded into lab_search
usage: %prog
"""

import boto3
from spark.spark_setup import init
from uuid import uuid4
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os
from spark.helpers.s3_utils import parse_s3_path

UUID = str(uuid4())
S3_TEMP_PATH = 's3://salusv/warehouse/backup/weekly/loinc_raw/'
S3_CONCAT_PATH = 's3://salusv/warehouse/backup/weekly/loinc_concat/'

S3_DESTINATION = {
    'Bucket': 'salusv',
    'Key': 'marketplace/search/lab/lab.psv.gz'
}

# init
conf_parameters = {
    'spark.sql.catalogImplementation': 'hive',
    'spark.default.parallelism': 2000,
    'spark.sql.shuffle.partitions': 2000,
    'spark.executor.memoryOverhead': 1024,
    'spark.driver.memoryOverhead': 1024
}

spark, sql_context = init("marketplace-pull-loinc", conf_parameters=conf_parameters)


def upload_psv_to_s3(source_bucket, source_key, dest_bucket, dest_key):
    s3 = boto3.client()

    list_obj_paginator = s3.get_paginator('list_objects_v2')
    source_key_file = None
    for page in list_obj_paginator.paginate(Bucket=source_bucket, Prefix=source_key):
        if 'Contents' in page:
            for i, file_obj in enumerate(
                    [x for x in page['Contents'] if x['Key'].endswith('.csv.gz')], 1):
                if i > 1:
                    raise Exception("More files than expected. Exiting.")
                source_key_file = file_obj['Key']

    if source_key_file:
        s3.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key_file},
                       Bucket=dest_bucket, Key=dest_key)


def pull_loinc():
    s3_temp_dir = os.path.join(S3_TEMP_PATH, UUID)
    s3_concat_dir = os.path.join(S3_CONCAT_PATH, UUID)

    get_ref_loinc = """
    SELECT DISTINCT 
        trim(loinc_num) AS loinc_code
        , trim(component) AS loinc_component
        , trim(long_common_name) AS long_common_name
        , loinc_system AS body_system
        , trim(loinc_class) AS loinc_class 
        , trim(method_type) AS loinc_method
        , trim(relatednames2) AS related
    FROM ref_loinc
    """

    rank_window = Window.orderBy(F.col("claims").desc(), F.col("loinc_code").asc())

    loinc_popularity = spark.read.parquet(
        's3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider=quest/') \
        .select("loinc_code", "claim_id").groupBy("loinc_code") \
        .agg(F.approx_count_distinct('claim_id').alias('claims'))

    ref_loinc = spark.sql(get_ref_loinc)
    output_columns = ref_loinc.columns + ["ordernum"]

    df = ref_loinc.join(loinc_popularity, "loinc_code", 'left_outer') \
        .withColumn("ordernum", F.row_number().over(rank_window)) \
        .select(output_columns)

    df.write \
        .option('sep', '|') \
        .option('header', False) \
        .csv(s3_temp_dir)

    combined = spark.read.csv(s3_temp_dir)
    combined.coalesce(1).write.csv(s3_concat_dir)

    source_bucket, source_key = parse_s3_path(S3_CONCAT_PATH)

    upload_psv_to_s3(source_bucket, source_key, S3_DESTINATION['Bucket'], S3_DESTINATION['Key'])


if __name__ == "__main__":
    sys.exit(pull_loinc())
