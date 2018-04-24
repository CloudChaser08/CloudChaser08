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

from pyspark.sql import SparkSession


spark = SparkSession.builder.master("yarn").appName("marketplace-ndc-pull").config('spark.sql.catalogImplementation', 'hive').getOrCreate()


def pull_ndc():
    with open('marketplace_ndc.psv', 'w') as ndc_out:
        csv_writer = csv.writer(ndc_out, delimiter='|')
        ndc_table = spark.sql("select * from marketplace_ndc").collect()
        for row in ndc_table:
            csv_writer.writerow(row)

    with open('marketplace_ndc.psv', 'rb') as f_in, gzip.open('ndc.psv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

if __name__ == "__main__":
    sys.exit(pull_ndc())
