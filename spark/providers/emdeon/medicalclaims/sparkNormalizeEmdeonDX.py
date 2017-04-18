#! /usr/bin/python
import os
import argparse
import time
from datetime import timedelta, datetime, date
from spark.runner import Runner
from spark.spark import init
import spark.helpers.normalized_records_unloader as normalized_records_unloader
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
spark, sqlContext = init("Emdeon DX")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3a://salusv/incoming/medicalclaims/emdeon/'
S3_EMDEON_OUT = 's3://salusv/warehouse/text/medicalclaims/2017-02-24/part_provider=emdeon/'
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
    if args.date < '2015-10-01':
        runner.run_spark_script(get_rel_path('../../../common/load_hvid_parent_child_map.sql'))

date_path = args.date.replace('-', '/')

runner.run_spark_script(get_rel_path('../../../common/medicalclaims_common_model.sql'), [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])
if args.date < '2015-08-01':
    runner.run_spark_script(get_rel_path('load_transactions.sql'), [
        ['input_path', S3_EMDEON_IN + date_path + '/payload/']
    ])
else:
    runner.run_spark_script(get_rel_path('load_transactions.sql'), [
        ['input_path', S3_EMDEON_IN + date_path + '/']
    ])

# before 2015-10-01 we did not include the parentId in the matching
# payload for exact matches, so there is a separate table to
# reconcile that
if args.date < '2015-10-01':
    runner.run_spark_script(get_rel_path('load_matching_payload_v1.sql'), [
        ['matching_path', S3_EMDEON_MATCHING + date_path + '/']
    ])
else:
    runner.run_spark_script(get_rel_path('load_matching_payload_v2.sql'), [
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

normalized_records_unloader.unload(spark, runner, 'medicalclaims', 'emdeon', 'date_service', args.date, S3_EMDEON_OUT)
spark.sparkContext.stop()
