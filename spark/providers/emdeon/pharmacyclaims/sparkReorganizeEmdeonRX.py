#! /usr/bin/python
import os
import argparse
import time
from spark.runner import Runner
from spark.spark_setup import init
import subprocess


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )


# init
spark, sqlContext = init("Emdeon RX")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3a://salusv/warehouse/text/pharmacyclaims/emdeon/'
S3_EMDEON_WAREHOUSE = 's3://salusv/warehouse/text/pharmacyclaims/2017-02-28/part_provider=emdeon/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
args = parser.parse_args()

date_path = args.date.replace('-', '/')
if args.date in ['2013-02-20', '2013-02-21', '2013-02-22'] or args.date > '2015-07-31':
    runner.run_spark_script(get_rel_path('load_warehouse_data.sql'), [
        ['input_path', S3_EMDEON_IN + date_path + '/']
    ])
else:
    runner.run_spark_script(get_rel_path('load_warehouse_data.sql'), [
        ['input_path', S3_EMDEON_IN + date_path + '/payload/']
    ])

runner.run_spark_script(get_rel_path('../../../common/pharmacyclaims_common_model.sql'))
runner.run_spark_script(get_rel_path('fix_data_types_and_ids.sql'), [
    ['min_date', '2012-01-01'],
    ['max_date', args.date]
])

runner.run_spark_script(get_rel_path('../../../common/pharmacyclaims_unload_table.sql'), [
    ['table_location', '/text/pharmacyclaims/emdeon/']
])
runner.run_spark_script(get_rel_path('../../../common/unload_common_model.sql'), [
    ['select_statement',
     "SELECT *, 'NULL' as part_best_date FROM pharmacyclaims_common_model WHERE date_service is NULL", False]
])
runner.run_spark_script(get_rel_path('../../../common/unload_common_model.sql'), [
    ['select_statement',
     "SELECT *, regexp_replace(date_service, '-..$', '') as part_best_date "
     "FROM pharmacyclaims_common_model WHERE date_service IS NOT NULL", False]
])

spark.sparkContext.stop()
subprocess.check_call(['hadoop', 'fs', '-get', '/text/pharmacyclaims/emdeon', './'])
dirs = subprocess.check_output(['ls', 'emdeon']).strip().split("\n")
for d in dirs:
    files = subprocess.check_output(['ls', 'emdeon/{}'.format(d)]).strip().split("\n")
    for f in files:
        subprocess.check_call(['mv', 'emdeon/{}/{}'.format(d, f), 'emdeon/{}/{}_{}'.format(d, args.date, f)])
subprocess.check_call(['aws', 's3', 'cp', '--sse', 'AES256', '--recursive', 'emdeon', S3_EMDEON_WAREHOUSE])
subprocess.check_call(['rm', '-r', 'emdeon'])
subprocess.check_call(['hadoop', 'fs', '-rm', '-r', '/text'])
