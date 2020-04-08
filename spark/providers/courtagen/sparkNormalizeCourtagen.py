#! /usr/bin/python
import argparse
import time
import logging
from datetime import timedelta, datetime
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


# init
spark, sqlContext = init("Courtagen")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_COURTAGEN_IN = 's3a://salusv/incoming/labtests/courtagen/'
S3_COURTAGEN_OUT = 's3a://salusv/warehouse/parquet/labtests/2017-04-19/'
S3_COURTAGEN_MATCHING = 's3a://salusv/matching/payload/labtests/courtagen/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
args = parser.parse_args()

setid = args.date + '_plain.out'

min_date = '2011-01-01'
max_date = args.date

# create helper tables
runner.run_spark_script('create_helper_tables.sql')

runner.run_spark_script('../../common/lab_common_model_v2.sql', [
    ['table_name', 'lab_common_model', False],
    ['properties', '', False]
])

date_path = args.date.replace('-', '/')

payload_loader.load(runner, S3_COURTAGEN_MATCHING + date_path + '/', ['hvJoinKey', 'claimId'])

runner.run_spark_script('load_transactions.sql', [
    ['input_path', S3_COURTAGEN_IN + date_path + '/']
])

runner.run_spark_script('normalize.sql', [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '28'],
    ['vendor', '59'],
    ['min_date', min_date],
    ['max_date', max_date]
])

runner.run_spark_script('../../common/lab_post_normalization_cleanup_v2.sql')

normalized_records_unloader.unload(
    spark, runner, 'labtests', 'lab_common_model_v2.sql', 'courtagen', 'lab_common_model',
    'date_service', args.date, S3_COURTAGEN_OUT
)

logger.log_run_details(
    provider_name='Courtagen',
    data_type=DataType.LAB_TESTS,
    data_source_transaction_path=S3_COURTAGEN_IN,
    data_source_matching_path=S3_COURTAGEN_MATCHING,
    output_path=S3_COURTAGEN_OUT,
    run_type=RunType.MARKETPLACE,
    input_date=args.date
)

spark.sparkContext.stop()

RunRecorder().record_run_details()
