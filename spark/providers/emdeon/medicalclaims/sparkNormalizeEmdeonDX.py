#! /usr/bin/python
import argparse
import time
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.normalized_records_unloader as normalized_records_unloader

# init
spark, sqlContext = init("Emdeon DX")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3a://salusv/incoming/medicalclaims/emdeon/'
S3_EMDEON_OUT = 's3://salusv/warehouse/parquet/medicalclaims/2017-02-24/'
S3_EMDEON_MATCHING = 's3a://salusv/matching/payload/medicalclaims/emdeon/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

setid = '{}_Claims_US_CF_D_deid.dat'.format(args.date.replace('-', ''))

runner.run_spark_script('create_helper_tables.sql')
runner.run_spark_script('../../../common/zip3_to_state.sql')
runner.run_spark_script('load_payer_mapping.sql')
if args.date < '2015-10-01':
    runner.run_spark_script('../../../common/load_hvid_parent_child_map.sql')

date_path = args.date.replace('-', '/')

runner.run_spark_script('../../../common/medicalclaims_common_model.sql', [
    ['table_name', 'medicalclaims_common_model', False],
    ['properties', '', False]
])
if args.date < '2015-08-01':
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', S3_EMDEON_IN + date_path + '/payload/']
    ])
else:
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', S3_EMDEON_IN + date_path + '/']
    ])

# before 2015-10-01 we did not include the parentId in the matching
# payload for exact matches, so there is a separate table to
# reconcile that
if args.date < '2015-10-01':
    runner.run_spark_script('load_matching_payload_v1.sql', [
        ['matching_path', S3_EMDEON_MATCHING + date_path + '/']
    ])
else:
    runner.run_spark_script('load_matching_payload_v2.sql', [
        ['matching_path', S3_EMDEON_MATCHING + date_path + '/']
    ])

runner.run_spark_script('split_raw_transactions.sql', [
    ['min_date', '2012-01-01'],
    ['max_date', args.date]
])
runner.run_spark_script('normalize_professional_claims.sql')
runner.run_spark_script('normalize_institutional_claims.sql')

# Privacy filtering
runner.run_spark_script('../../../common/medicalclaims_post_normalization_cleanup.sql', [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '10'],
    ['vendor', '11']
])

normalized_records_unloader.partition_and_rename(
    spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'emdeon',
    'medicalclaims_common_model', 'date_service', args.date
)
spark.sparkContext.stop()
normalized_records_unloader.distcp(S3_EMDEON_OUT)
