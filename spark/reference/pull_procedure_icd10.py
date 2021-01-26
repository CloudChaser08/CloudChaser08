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
from pyspark.sql import SparkSession

s3 = boto3.resource('s3')

S3_CONF = {
    'Bucket': 'salusv',
    'Key': 'marketplace/search/procedure_icd10/proc_icd10.psv.gz'
}

sql = """
select distinct trim(a.icd10order), trim(a.icd10code), trim(a.icd10desc)
        , trim(b.section_desc) as level1_desc, trim(c.position_description) as level2_desc
        , trim(d.position_description) as level3_desc, trim(e.position_description) as level4_desc
        , trim(f.position_description) as level5_desc, trim(g.position_description) as level6_desc
        , CASE WHEN h.icd9='NoPx' then NULL else h.icd9 end as icd9code, upper(i.long_description) as icd9desc
from
    (select ordernum as icd10order, code as icd10code, upper(long_description) as icd10desc,
            substring(code, 1, 1) as section,
            substring(code, 2, 1) as pos2,
            substring(code, 3, 1) as pos3,
            substring(code, 4, 1) as pos4,
            substring(code, 5, 1) as pos5,
            substring(code, 6, 1) as pos6
    from ref_icd10_procedure
    ) a
    left join (select distinct section, section_desc from ref_icd10_procedure_groups) b 
        on a.section=b.section
    left join (select * from ref_icd10_procedure_groups where position_num=2) c 
        on a.section=c.section and a.pos2=c.position_value
    left join (select * from ref_icd10_procedure_groups where position_num=3) d 
        on a.section=d.section and a.pos3=d.position_value
    left join (select * from ref_icd10_procedure_groups where position_num=4) e 
        on a.section=e.section and a.pos4=e.position_value
    left join (select * from ref_icd10_procedure_groups where position_num=5) f 
        on a.section=f.section and a.pos5=f.position_value
    left join (select * from ref_icd10_procedure_groups where position_num=6) g 
        on a.section=g.section and a.pos6=g.position_value
    left join ref_icd10_to_icd9_procedure h on a.icd10code=h.icd10
    left join (select * from ref_icd9_procedure) i on h.icd9=i.code
"""


spark = SparkSession.builder.master("yarn")\
    .appName("marketplace-procedure-pull").config('spark.sql.catalogImplementation', 'hive').getOrCreate()


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
