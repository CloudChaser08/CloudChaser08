#! /usr/bin/python
import os
import argparse
import time
from datetime import timedelta, datetime, date
from spark.runner import Runner
from spark.spark import init
import subprocess
import spark.helpers.create_date_validation_table \
    as date_validator
import logging


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )

# init
spark, sqlContext = init("Quest")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3a://salusv/incoming/medicalclaims/emdeon/'
S3_EMDEON_OUT = 's3://salusv/warehouse/text/medicalclaims/emdeon/'
S3_EMDEON_MATCHING = 's3a://salusv/matching/payload/medicalclaims/emdeon/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--first_run', default=False, action='store_true')
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

if args.first_run:
    runner.run_spark_script(get_rel_path('create_helper_tables.sql'))
    runner.run_spark_script(get_rel_path('../../../common/zip3_to_state.sql'))
    runner.run_spark_script(get_rel_path('load_payer_mapping.sql'))
    runner.run_spark_script(get_rel_path('../../../common/load_hvid_parent_child_map.sql'))

date_path = args.date.replace('-', '/')

runner.run_spark_script(get_rel_path('../../../common/medicalclaims_common_model.sql'))
runner.run_spark_script(get_rel_path('load_transactions.sql'), [
    ['input_path', S3_EMDEON_IN + date_path + '/']
])
runner.run_spark_script(get_rel_path('load_matching_payload.sql'), [
    ['matching_path', S3_EMDEON_MATCHING + date_path + '/']
])
runner.run_spark_script(get_rel_path('split_raw_transactions.sql'), [
    ['min_date', '2012-01-01'],
    ['max_date', args.date]
])
runner.run_spark_script(get_rel_path('normalize_professional_claims.sql'))
runner.run_spark_script(get_rel_path('normalize_institutional_claims.sql'))

# Privacy filtering
runner.run_spark_script(get_rel_path('../../../common/medicalclaims_post_normalization_cleanup.sql'), [
    ['filename', args.setid],
    ['today', TODAY],
    ['feedname', '10'],
    ['vendor', '11']
])

runner.run_spark_script(get_rel_path('../../../common/medicalclaims_unload_table.sql'), [
    ['table_location', '/text/medicalclaims/emdeon/']
])
runner.run_spark_script(get_rel_path('../../../common/unload_common_model.sql'), [
    ['select_statement', "SELECT *, 'NULL' as part_best_date FROM medicalclaims_common_model WHERE date_service is NULL", False]
])
runner.run_spark_script(get_rel_path('../../../common/unload_common_model.sql'), [
    ['select_statement', "SELECT *, regexp_replace(date_service, '-..$', '') as part_best_date FROM medicalclaims_common_model WHERE date_service IS NOT NULL", False]
])

part_files = subprocess.check_output(['hadoop', 'fs', '-ls', '-R', '/text/medicalclaims/emdeon/']).strip().split("\n")
def move_file(part_file):
    if part_file[-3:] == ".gz":
        old_pf = part_file.split(' ')[-1].strip()
        new_pf = '/'.join(old_pf.split('/')[:-1] + [args.date + '_' + old_pf.split('/')[-1]])
        subprocess.check_call(['hadoop', 'fs', '-mv', old_pf, new_pf])
spark.sparkContext.parallelize(part_files).foreach(move_file)
spark.sparkContext.stop()

subprocess.check_call(['s3-dist-cp', '--src', '/text/medicalclaims/emdeon/', '--dest', S3_EMDEON_OUT])
