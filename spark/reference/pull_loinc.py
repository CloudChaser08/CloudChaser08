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
from spark.spark_setup import init

s3 = boto3.resource('s3')
basestring = None

S3_CONF = {
    'Bucket': 'salusv',
    'Key': 'marketplace/search/lab/lab.psv.gz'
}

sql1 = """
SELECT 
    loinc_code
    , count(distinct claim_id) AS claims
FROM 
    dw._labtests_nb
WHERE part_provider = 'quest' 
    --AND loinc_code<>' ' 
    --AND loinc_code<>'' 
    --AND loinc_code is not null
    AND 0 <> LENGTH(TRIM(COALESCE(loinc_code, '')))
GROUP BY loinc_code
"""

sql2 = """
SELECT DISTINCT 
    trim(loinc_num) AS loinc_code
    , trim(component) AS loinc_component
    , trim(long_common_name) AS long_common_name
    , loinc_system AS body_system
    , trim(loinc_class) AS loinc_class 
    , trim(method_type) AS loinc_method
    , trim(relatednames2) AS related
    , row_number() OVER(ORDER BY b.claims desc, b.loinc_code) AS ordernum
FROM ref_loinc a
    LEFT OUTER JOIN loinc_popularity b ON a.loinc_num=b.loinc_code
ORDER BY 1
"""

# init
conf_parameters = {
    'spark.sql.catalogImplementation': 'hive',
    'spark.default.parallelism': 2000,
    'spark.sql.shuffle.partitions': 2000,
    'spark.executor.memoryOverhead': 1024,
    'spark.driver.memoryOverhead': 1024
}
spark, sql_context = init("marketplace-pull-loinc", conf_parameters=conf_parameters)

if not basestring:
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
