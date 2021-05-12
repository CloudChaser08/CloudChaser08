#!/usr/bin/env python2.7
"""
Create a compressed version of ndc data to be loaded into ndc_search
Requires pull_ndc_ref.sql to be run first in order to update/create marketplace_ndc
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
    'Key': 'marketplace/search/ndc/ndc.psv.gz'
}


# init
conf_parameters = {
    'spark.sql.catalogImplementation': 'hive',
    'spark.default.parallelism': 4000,
    'spark.sql.shuffle.partitions': 4000,
    'spark.executor.memoryOverhead': 1024,
    'spark.driver.memoryOverhead': 1024
}
spark, sql_context = init("marketplace-ndc-pull", conf_parameters=conf_parameters)


def pull_ndc():

    # Prepare tables
    with open('/home/hadoop/spark/reference/pull_ndc_ref.sql') as sql_fo:
        sqls = sql_fo.read().split(';')
        for statement in sqls[:-1]:  # skip last line containing \n
            spark.sql(statement)

    with open('marketplace_ndc.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        ndc_table = spark.sql("select * from marketplace_ndc").collect()
        for row in ndc_table:
            csv_writer.writerow(row)

    with open('marketplace_ndc.psv', 'rb') as f_in, gzip.open('ndc.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    s3.meta.client.upload_file('ndc.psv.gz', **S3_CONF)


if __name__ == "__main__":
    sys.exit(pull_ndc())
