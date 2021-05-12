#!/usr/bin/env python2.7
"""
Create a compressed version of icd10 procedures to be loaded into proc_icd10_search
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
    'Key': 'marketplace/search/procedure_icd10/proc_icd10.psv.gz'
}

sql = """
SELECT DISTINCT 
    trim(a.icd10order)
    , trim(a.icd10code)
    , trim(a.icd10desc)
    , trim(b.section_desc) AS level1_desc
    , trim(c.position_description) AS level2_desc
    , trim(d.position_description) AS level3_desc
    , trim(e.position_description) AS level4_desc
    , trim(f.position_description) AS level5_desc
    , trim(g.position_description) AS level6_desc
    , CASE WHEN h.icd9='NoPx' THEN NULL ELSE h.icd9 END AS icd9code
    , upper(i.long_description) AS icd9desc
FROM
    (
        SELECT 
            ordernum AS icd10order
            , code AS icd10code
            , upper(long_description) AS icd10desc
            , substring(code, 1, 1) AS section
            , substring(code, 2, 1) AS pos2
            , substring(code, 3, 1) AS pos3
            , substring(code, 4, 1) AS pos4
            , substring(code, 5, 1) AS pos5
            , substring(code, 6, 1) AS pos6
        FROM 
            ref_icd10_procedure
    ) a
    LEFT OUTER JOIN (select distinct section, section_desc from ref_icd10_procedure_groups) b 
        ON a.section=b.section
    LEFT OUTER JOIN (select * from ref_icd10_procedure_groups where position_num=2) c 
        ON a.section=c.section and a.pos2=c.position_value
    LEFT OUTER JOIN (select * from ref_icd10_procedure_groups where position_num=3) d 
        ON a.section=d.section and a.pos3=d.position_value
    LEFT OUTER JOIN (select * from ref_icd10_procedure_groups where position_num=4) e 
        ON a.section=e.section and a.pos4=e.position_value
    LEFT OUTER JOIN (select * from ref_icd10_procedure_groups where position_num=5) f 
        ON a.section=f.section and a.pos5=f.position_value
    LEFT OUTER JOIN (select * from ref_icd10_procedure_groups where position_num=6) g 
        ON a.section=g.section and a.pos6=g.position_value
    LEFT OUTER JOIN ref_icd10_to_icd9_procedure h 
        ON a.icd10code=h.icd10
    LEFT OUTER JOIN (select * from ref_icd9_procedure) i 
        ON h.icd9=i.code
"""

# init
conf_parameters = {
    'spark.sql.catalogImplementation': 'hive',
    'spark.default.parallelism': 4000,
    'spark.sql.shuffle.partitions': 4000,
    'spark.executor.memoryOverhead': 1024,
    'spark.driver.memoryOverhead': 1024
}
spark, sql_context = init("marketplace-procedure-pull", conf_parameters=conf_parameters)


def pull_procedure_icd10():
    with open('marketplace_proc_icd10.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        ndc_table = spark.sql(sql).collect()
        for row in ndc_table:
            csv_writer.writerow(row)

    with open('marketplace_proc_icd10.psv', 'rb') as f_in, gzip.open('proc_icd10.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    s3.meta.client.upload_file('proc_icd10.psv.gz', **S3_CONF)


if __name__ == "__main__":
    sys.exit(pull_procedure_icd10())
