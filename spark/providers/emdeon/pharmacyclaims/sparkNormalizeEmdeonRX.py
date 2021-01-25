#! /usr/bin/python
import os
import argparse
import time
from datetime import timedelta, datetime
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

# S3_EMDEON_IN = 's3a://salusv/incoming/pharmacyclaims/emdeon/'
# S3_EMDEON_WAREHOUSE = 's3a://salusv/warehouse/text/pharmacyclaims/2017-02-28/part_provider=emdeon/'
# S3_EMDEON_OUT = 's3://healthveritydev/ifishbein/emdeon_rx_emr_test/'
# S3_EMDEON_MATCHING = 's3a://salusv/matching/payload/pharmacyclaims/emdeon/'

# S3_EMDEON_IN = 's3a://salusv/incoming/pharmacyclaims/emdeon/'
# S3_EMDEON_MATCHING = 's3a://salusv/matching/payload/pharmacyclaims/emdeon/'
S3_EMDEON_WAREHOUSE = 's3a://salusv/warehouse/text/pharmacyclaims/2017-02-28/part_provider=emdeon/'
S3_EMDEON_MATCHING = "s3://salusv/sample/changehealthcare/rx/payload/"
S3_EMDEON_OUT = "s3://salusv/sample/changehealthcare/rx/normalized/"
S3_EMDEON_IN = "s3://salusv/sample/changehealthcare/rx/transactions/"

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--first_run', default=False, action='store_true')
parser.add_argument('--debug', default=False, action='store_true')
parser.add_argument('--sample', default=False, action='store_true')
args = parser.parse_args()

if args.first_run:
    runner.run_spark_script(get_rel_path('../../../common/zip3_to_state.sql'))
    runner.run_spark_script(get_rel_path('load_payer_mapping.sql'))
    if args.date < '2016-10-01':
        runner.run_spark_script(get_rel_path('../../../common/load_hvid_parent_child_map.sql'))

if not args.sample:
    file_date = datetime.strptime(args.date, '%Y-%m-%d')
    runner.run_spark_script(get_rel_path('data_to_reverse_table.sql'), [
        ['normalized_data_path', S3_EMDEON_WAREHOUSE],
    ])
    partitions_to_load = set([
        file_date.strftime('part_best_date=%Y-%m'),
        (file_date - timedelta(days=14)).strftime('part_best_date=%Y-%m')
    ])
    for d_path in partitions_to_load:
        runner.run_spark_script(get_rel_path('load_normalized_data.sql'), [
            ['best_date', d_path],
            ['date_path', S3_EMDEON_WAREHOUSE + d_path + '/']
        ])

runner.run_spark_script(get_rel_path('../../../common/pharmacyclaims/sql/pharmacyclaims_common_model_v1.sql'))
if args.sample:
    print("running load_transactions_v2")
    date_path = args.date.replace('-', '_')  # this was modified for the sample
    runner.run_spark_script(get_rel_path('load_transactions_v2.sql'), [
        ['input_path', S3_EMDEON_IN + date_path + '/RX_DEID_CF_ON/']  # note this was added for the sample
    ])
else:
    date_path = args.date.replace('-', '/')

    if args.date < '2016-12-13':
        if args.date < '2015-08-01':
            runner.run_spark_script(get_rel_path('load_transactions_v1.sql'), [
                ['input_path', S3_EMDEON_IN + date_path + '/payload']
            ])
        else:
            runner.run_spark_script(get_rel_path('load_transactions_v1.sql'), [
                ['input_path', S3_EMDEON_IN + date_path + '/']
            ])
    else:
        runner.run_spark_script(get_rel_path('load_transactions_v2.sql'), [
            ['input_path', S3_EMDEON_IN + date_path + '/']
        ])

print("running trim_format_raw_transactions")
runner.run_spark_script(get_rel_path('trim_format_raw_transactions.sql'), [
    ['min_date', '2012-01-01'],
    ['max_date', args.date]
])
if args.date < '2016-10-01':
    runner.run_spark_script(get_rel_path('load_matching_payload_v1.sql'), [
        ['matching_path', S3_EMDEON_MATCHING + date_path + '/']
    ])
else:
    print("running load_matching_payload_v2")

    runner.run_spark_script(get_rel_path('load_matching_payload_v2.sql'), [
        ['matching_path', S3_EMDEON_MATCHING + date_path + '/']
    ])

if args.sample:
    print("running normalize_pharmacy_claims")

    runner.run_spark_script(get_rel_path('normalize_pharmacy_claims.sql'), [
        ['filename', 'sample_file'],
        ['today', TODAY],
        ['feedname', '11'],
        ['vendor', '35']
    ])

else:
    runner.run_spark_script(get_rel_path('normalize_pharmacy_claims.sql'), [
        ['filename', args.setid],
        ['today', TODAY],
        ['feedname', '11'],
        ['vendor', '35']
    ])

print("running pharmacyclaims_post_normalization_cleanup")
runner.run_spark_script(get_rel_path('../../../common/pharmacyclaims_post_normalization_cleanup.sql'))

if not args.sample:
    print("running clean_out_reversed_claims")
    runner.run_spark_script(get_rel_path('clean_out_reversed_claims.sql'))

print("running pharmacyclaims_unload_table")
runner.run_spark_script(get_rel_path('../../../common/pharmacyclaims_unload_table.sql'), [
    ['table_location', '/text/pharmacyclaims/emdeon/']
])

old_partition_count = spark.conf.get('spark.sql.shuffle.partitions')
print("running unload_common_model")
runner.run_spark_script(get_rel_path('../../../common/unload_common_model.sql'), [
    ['select_statement',
     "SELECT *, 'NULL' as part_best_date FROM pharmacyclaims_common_model WHERE date_service is NULL", False],
    ['unload_partition_count', 20, False],
    ['original_partition_count', old_partition_count, False],
    ['distribution_key', 'record_id', False]
])

print("running unload_common_model again")
runner.run_spark_script(get_rel_path('../../../common/unload_common_model.sql'), [
    ['select_statement',  "SELECT *, regexp_replace(date_service, '-..$', '') as part_best_date "
     "FROM pharmacyclaims_common_model WHERE date_service IS NOT NULL", False],
    ['unload_partition_count', 20, False],
    ['original_partition_count', old_partition_count, False],
    ['distribution_key', 'record_id', False]
])

print("running list files")
part_files = subprocess.check_output(
    ['hadoop', 'fs', '-ls', '-R', '/text/pharmacyclaims/emdeon/']).decode('utf8').strip().split("\n")


def move_file(part_file):
    if part_file[-3:] == ".gz":
        old_pf = part_file.split(' ')[-1].strip()
        new_pf = '/'.join(old_pf.split('/')[:-1] + [args.date + '_' + old_pf.split('/')[-1]])
        subprocess.check_call(['hadoop', 'fs', '-mv', old_pf, new_pf])


print("running move files")
spark.sparkContext.parallelize(part_files).foreach(move_file)
spark.sparkContext.stop()

print("running copy to output")
subprocess.check_call(['s3-dist-cp', '--src', '/text/pharmacyclaims/emdeon/', '--dest', S3_EMDEON_OUT])
