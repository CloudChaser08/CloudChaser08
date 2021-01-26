#!/usr/bin/env python2.7
"""
Create a compressed version of LOINC to be loaded into lab_search
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
    'Key': 'marketplace/search/lab/lab.psv.gz'
}

sql1 = """
select loinc_code, count(distinct claim_id) as claims
    from labtests
    where part_provider = 'quest' and loinc_code<>' ' and loinc_code<>'' and loinc_code is not null
    group by loinc_code
"""

sql2 = """
select distinct trim(loinc_num) as loinc_code, trim(component) as loinc_component
    , trim(long_common_name), loinc_system as body_system, trim(loinc_class)
    , trim(method_type) as loinc_method, trim(relatednames2) as related
    , row_number() OVER(ORDER BY b.claims desc, b.loinc_code) as ordernum
from ref_loinc a
    left join loinc_popularity b on a.loinc_num=b.loinc_code
order by 1
"""


spark = SparkSession.builder.master("yarn").appName(
    "marketplace-pull-loinc").config('spark.sql.catalogImplementation', 'hive').getOrCreate()
try:
    basestring
except:
    basestring = str


def pull_loinc():
    with open('marketplace_lab.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        spark.sql(sql1).cache().createOrReplaceTempView('loinc_popularity')
        spark.table('loinc_popularity').count()
        ndc_table = spark.sql(sql2).collect()
        for row in ndc_table:
            try:
                csv_writer.writerow(row)
            except:
                encoded_row = []
                for c in row:
                    if isinstance(c, basestring):
                        c = c.encode('utf-8')
                    encoded_row.append(c)
                csv_writer.writerow(encoded_row)

    with open('marketplace_lab.psv', 'rb') as f_in, gzip.open('lab.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    s3.meta.client.upload_file('lab.psv.gz', **S3_CONF)


if __name__ == "__main__":
    sys.exit(pull_loinc())
