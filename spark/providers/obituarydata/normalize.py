import argparse
import time
from datetime import datetime, date
from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.create_date_validation_table \
    as date_validator
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


S3_PATH = 's3://salusv/warehouse/parquet/consumer/2017-02-23/'


# init
spark, sqlContext = init("ObituaryData")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_known_args()[0]

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

filename = 'OD_record_data_{}_'.format(args.date.replace('-', ''))

input_path = 's3a://salusv/incoming/consumer/obituarydata/{}/'.format(
    args.date.replace('-', '/')
)

matching_path = 's3a://salusv/matching/payload/consumer/obituarydata/{}/' \
                .format(
                    args.date.replace('-', '/')
                )
output_path = 'hdfs:///out/'

# create date table
date_validator.generate(runner, date(2013, 9, 1), date_obj.date())

runner.run_spark_script('../../common/event_common_model.sql', [
    ['table_name', 'event_common_model', False],
    ['properties', '', False]
])

payload_loader.load(runner, matching_path, ['hvJoinKey', 'deathMonth'])

runner.run_spark_script("load_transactions.sql", [
    ['input_path', input_path]
])

runner.run_spark_script("normalize.sql", [
    ['set', filename],
    ['feed', '27'],
    ['vendor', '49']
])

runner.run_spark_script('../../common/event_common_model.sql', [
    ['table_name', 'final_unload', False],
    [
        'properties',
        constants.unload_properties_template.format('part_provider', 'part_best_date', output_path),
        False
    ]
])

old_partition_count = spark.conf.get('spark.sql.shuffle.partitions')

runner.run_spark_script('../../common/unload_common_model.sql', [
    [
        'select_statement',
        "SELECT *, 'obituarydata' as part_provider, 'NULL' as best_date "
        + "FROM event_common_model "
        + "WHERE event_date IS NULL",
        False
    ],
    ['unload_partition_count', '20', False],
    ['original_partition_count', old_partition_count, False],
    ['distribution_key', 'record_id', False]
])

runner.run_spark_script('../../common/unload_common_model.sql', [
    [
        'select_statement',
        "SELECT *, 'obituarydata' as part_provider, "
        + "regexp_replace(cast(event_date as string), '-..$', '') as best_date "
        + "FROM event_common_model "
        + "WHERE event_date IS NOT NULL",
        False
    ],
    ['unload_partition_count', '20', False],
    ['original_partition_count', old_partition_count, False],
    ['distribution_key', 'record_id', False]
])

if not args.debug:
    logger.log_run_details(
        provider_name='Obituary',
        data_type=DataType.CONSUMER,
        data_source_transaction_path=input_path,
        data_source_matching_path=matching_path,
        output_path=S3_PATH,
        run_type=RunType.MARKETPLACE,
        input_date=args.date
    )

spark.stop()

hadoop_time = normalized_records_unloader.timed_distcp(S3_PATH, src=output_path)
RunRecorder().record_run_details(additional_time=hadoop_time)
