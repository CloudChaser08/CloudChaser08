#!/usr/bin/env python3.4
"""
Create a compressed version of LOINC to be loaded into lab_search
usage:
$ /bin/sh -c "PYSPARK_PYTHON=python3 /home/hadoop/spark/bin/run.sh \
    /home/hadoop/spark/reference/pull_loinc.py"
"""

import subprocess
import sys
import boto3
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from spark.spark_setup import init
import spark.helpers.hdfs_utils as hdfs_utils

loinc_ref = 'default.ref_loinc'  # HV default DB
dw_quest_labtests_loc = 's3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider=quest/'

ref_file_name = 'lab.psv.gz'
stage_ref_loc = '/staging/'
s3 = boto3.resource('s3')

S3_CONF = {
    'Bucket': 'salusv',
    'Key': 'marketplace/search/lab/{}'.format(ref_file_name)
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


def pull_loinc():
    print('Collect default.ref_loinc')
    get_ref_loinc = """
    SELECT DISTINCT
        trim(loinc_num) AS loinc_code
        , trim(component) AS loinc_component
        , trim(long_common_name) AS long_common_name
        , loinc_system AS body_system
        , trim(loinc_class) AS loinc_class
        , trim(method_type) AS loinc_method
        , trim(relatednames2) AS related
    FROM {}
    """.format(loinc_ref)

    rank_window = Window.orderBy(F.col("claims").desc(), F.col("loinc_code").asc())

    print('Collect quest data from dw.labtests')
    loinc_popularity = spark.read.parquet(dw_quest_labtests_loc) \
        .select("loinc_code", "claim_id").groupBy("loinc_code") \
        .agg(F.approx_count_distinct('claim_id').alias('claims'))

    ref_loinc = spark.sql(get_ref_loinc)
    output_columns = ref_loinc.columns + ["ordernum"]

    df = ref_loinc.join(loinc_popularity, "loinc_code", 'left_outer') \
        .withColumn("ordernum", F.row_number().over(rank_window)) \
        .select(output_columns)

    print('Write output into {}'.format(stage_ref_loc))
    df.coalesce(1).write \
        .mode("overwrite") \
        .option('sep', '|') \
        .option('compression', 'gzip') \
        .option('header', False) \
        .csv(stage_ref_loc)

    spark.stop()
    print('File rename to {}'.format(ref_file_name))
    stg_file_name = [f for f in hdfs_utils.get_files_from_hdfs_path('hdfs://' + stage_ref_loc)
                     if not f.startswith('.') and f != "_SUCCESS" and f.endswith('.csv.gz')][0]
    hdfs_utils.rename_file_hdfs(stage_ref_loc + stg_file_name, stage_ref_loc + ref_file_name)

    print('File transferred from hdfs {} to local {}'.format(stage_ref_loc + ref_file_name, ref_file_name))
    subprocess.check_call(['hdfs', 'dfs', '-get', '-f', stage_ref_loc + ref_file_name, ref_file_name])

    print('File transferred from local {} to prod s3 {}'.format(ref_file_name, S3_CONF))
    s3.meta.client.upload_file(ref_file_name, **S3_CONF)

    print('Done')


if __name__ == "__main__":
    sys.exit(pull_loinc())
