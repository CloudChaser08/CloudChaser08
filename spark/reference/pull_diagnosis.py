#!/usr/bin/env python2.7
"""
Create a compressed version of diagnosis codes to be loaded into diag_search
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
    'Key': 'marketplace/search/diagnosis/diag.psv.gz'
}

sql = """
select distinct trim(a.code) as icd10code, trim(upper(a.long_description)) as icd10desc,
        CASE WHEN c.icd9='NoDx' then NULL else c.icd9 end as icd9code,
        upper(d.long_description) as icd9desc,
        concat(f.start_num, '-', f.stop_num) as level1, f.description as level1_desc,
        concat(g.start_num, '-', g.stop_num) as level2, g.description as level2_desc,
        b.code as level3, upper(b.long_description) as level3_desc
from ref_icd10_diagnosis a
    left join
    (select distinct code, long_description
    from ref_icd10_diagnosis
    where length(trim(code))=3
    ) b on substring(a.code, 1,  3)=b.code
    left join ref_icd10_to_icd9_diagnosis c on a.code=c.icd10
    left join ref_icd10_diagnosis d on c.icd9=d.code
    left join (select * from ref_icd10_diagnosis_groups where level_num=1) f on b.code between f.start_num and f.stop_num
    left join (select * from ref_icd10_diagnosis_groups where level_num=2) g on (
        (b.code between g.start_num and g.stop_num) or
        (b.code = 'M1A' and g.start_num = 'M05' and g.stop_num = 'M14') or
        (b.code = 'Z3A' and g.start_num = 'Z30' and g.stop_num = 'Z39') or
        (b.code = 'C4A' and g.start_num = 'C43' and g.stop_num = 'C44')
    )
where
    a.header = 1 -- indicates billable
or
    trim(b.code) = 'U07' -- allow covid emergency codes, even if not billable
"""


spark = SparkSession.builder.master("yarn").appName("marketplace-pull-diagnosis").config('spark.sql.catalogImplementation', 'hive').getOrCreate()


def pull_diagnosis():
    with open('marketplace_diag.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        ndc_table = spark.sql(sql).collect()
        for row in ndc_table:
            csv_writer.writerow(row)

    with open('marketplace_diag.psv', 'rb') as f_in, gzip.open('diag.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    s3.meta.client.upload_file('diag.psv.gz', **S3_CONF)

if __name__ == "__main__":
    sys.exit(pull_diagnosis())
