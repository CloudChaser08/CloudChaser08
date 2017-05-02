#! /usr/bin/python
import argparse
import time
import logging
from datetime import timedelta, datetime
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_prefix as file_prefix
import spark.helpers.constants as constants
import spark.helpers.normalized_records_unloader as normalized_records_unloader

# init
spark, sqlContext = init("Courtagen")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_COURTAGEN_IN = 's3a://salusv/incoming/labtests/courtagen/'
S3_COURTAGEN_OUT = 's3a://salusv/warehouse/text/labtests/2017-04-19/'
S3_COURTAGEN_MATCHING = 's3a://salusv/matching/payload/labtests/courtagen/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
args = parser.parse_args()

setid = args.date + '_plain.out'

min_date = '2011-01-01'
max_date = args.date

# create helper tables
runner.run_spark_script(file_utils.get_rel_path(
    __file__,
    'create_helper_tables.sql'
))

runner.run_spark_script(file_utils.get_rel_path(
        __file__,
        '../../common/lab_common_model_v2.sql'
    ), [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
])

date_path = args.date.replace('-', '/')

payload_loader.load(runner, S3_COURTAGEN_MATCHING + date_path + '/', ['hvJoinKey', 'claimId'])

runner.run_spark_script(
    file_utils.get_rel_path(
        __file__, 'load_transactions.sql'
    ), [
        ['input_path', S3_COURTAGEN_IN + date_path + '/']
])

runner.run_spark_script(file_utils.get_rel_path(
    __file__, 'normalize.sql'
), [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '28'],
    ['vendor', '59'],
    ['min_date', min_date],
    ['max_date', max_date]
])

runner.run_spark_script(file_utils.get_rel_path(
    __file__, '../../common/lab_post_normalization_cleanup_v2.sql'
))

normalized_records_unloader.unload(spark, runner, 'labtests', 'lab_common_model_v2.sql',
    'courtagen', 'lab_common_model', 'date_service', args.date, S3_COURTAGEN_OUT)

spark.sparkContext.stop()
