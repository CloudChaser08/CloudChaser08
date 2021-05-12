#!/usr/bin/env python2.7
"""
Create a compressed version of hcpcs to be loaded into proc_search
usage: %prog
"""

import csv
import gzip
import shutil
import sys

import boto3
from spark.spark_setup import init

s3 = boto3.resource('s3')

S3_CONF = {
    'Bucket': 'salusv',
    'Key': 'marketplace/search/procedure/proc.psv.gz'
}

# init
conf_parameters = {
    'spark.sql.catalogImplementation': 'hive',
    'spark.default.parallelism': 4000,
    'spark.sql.shuffle.partitions': 4000,
    'spark.executor.memoryOverhead': 1024,
    'spark.driver.memoryOverhead': 1024
}
spark, sql_context = init("marketplace-pull-hcpcs", conf_parameters=conf_parameters)


def pull_hcpcs():
    with open('marketplace_proc.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        ndc_table = spark.sql("SELECT DISTINCT trim(hcpc), trim(long_description) FROM ref_hcpcs").collect()
        for row in ndc_table:
            csv_writer.writerow(row)

        ndc_table = spark.sql("SELECT DISTINCT trim(code), trim(long_description) FROM ref_cpt").collect()
        for row in ndc_table:
            csv_writer.writerow(row)

    with open('marketplace_proc.psv', 'rb') as f_in, gzip.open('proc.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    s3.meta.client.upload_file('proc.psv.gz', **S3_CONF)


if __name__ == "__main__":
    sys.exit(pull_hcpcs())
