import os
import argparse
import time
from datetime import datetime, date
from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.create_date_validation_table \
    as date_validator
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants

# init
spark, sqlContext = init("ObituaryData")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

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
        constants.unload_properties_template.format(output_path),
        False
    ]
])

runner.run_spark_script('../../common/unload_common_model.sql'), [
    [
        'select_statement',
        "SELECT *, 'NULL' as best_date "
        + "FROM event_common_model "
        + "WHERE event_date IS NULL",
        False
    ],
    ['partitions', '20', False]
])

runner.run_spark_script('../../common/unload_common_model.sql'), [
    [
        'select_statement',
        "SELECT *, regexp_replace(cast(event_date as string), '-..$', '') as best_date "
        + "FROM event_common_model "
        + "WHERE event_date IS NOT NULL",
        False
    ],
    ['partitions', '20', False]
])

spark.stop()
