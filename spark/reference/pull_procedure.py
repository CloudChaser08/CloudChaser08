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
from pyspark.sql import SparkSession

s3 = boto3.resource('s3')

S3_CONF = {
    'Bucket': 'salusv',
    'Key': 'marketplace/search/procedure/proc.psv.gz'
}


spark = SparkSession.builder.master("yarn").appName(
    "marketplace-pull-hcpcs").config('spark.sql.catalogImplementation', 'hive').getOrCreate()


def pull_hcpcs():
    with open('marketplace_proc.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        ndc_table = spark.sql("select distinct trim(hcpc), trim(long_description) from ref_hcpcs").collect()
        for row in ndc_table:
            csv_writer.writerow(row)

        ndc_table = spark.sql("select distinct trim(code), trim(long_description) from ref_cpt").collect()
        for row in ndc_table:
            csv_writer.writerow(row)

    with open('marketplace_proc.psv', 'rb') as f_in, gzip.open('proc.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    s3.meta.client.upload_file('proc.psv.gz', **S3_CONF)


if __name__ == "__main__":
    sys.exit(pull_hcpcs())
