#! /usr/bin/python
import os
import argparse
import time
from datetime import timedelta, datetime, date
from pyspark.sql import HiveContext, SparkSession
from providers.spark_norm_common.runner import Runner
from providers.spark_norm_common.user_defined_functions \
    import get_diagnosis_with_priority, string_set_diff, uniquify
from providers.spark_norm_common.post_normalization_cleanup \
    import clean_up_diagnosis_code, obscure_place_of_service, \
    filter_due_to_place_of_service
import providers.spark_norm_common.create_date_validation_table \
    as date_validator


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )

# init
spark = SparkSession.builder                                            \
                    .master("yarn")                                     \
                    .appName("Quest Normalization")                     \
                    .config('spark.sql.catalogImplementation', 'hive')  \
                    .getOrCreate()

sqlContext = HiveContext(spark.sparkContext)

# register udfs
sqlContext.registerFunction(
    'get_diagnosis_with_priority', get_diagnosis_with_priority
)
sqlContext.registerFunction(
    'string_set_diff', string_set_diff
)
sqlContext.registerFunction(
    'uniquify', uniquify
)

# register privacy filters
sqlContext.registerFunction(
    'filter_due_to_place_of_service', filter_due_to_place_of_service
)
sqlContext.registerFunction(
    'obscure_place_of_service', obscure_place_of_service
)
sqlContext.registerFunction(
    'clean_up_diagnosis_code', clean_up_diagnosis_code
)

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--period', type=str)
parser.add_argument('--date', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

setid = 'HealthVerity_' + \
        date_obj.strftime('%Y%m%d') + \
        (date_obj + timedelta(days=1)).strftime('%m%d')

input_path = 's3://salusv/incoming/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
trunk_path = input_path + 'trunk/'
addon_path = input_path + 'addon/'

matching_path = 's3://salusv/matching/payload/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
output_path = 'hdfs:///out/'

# create helper tables
runner.enqueue_psql_script(get_rel_path(
    'create_helper_tables.sql'
))

# create date table
date_validator.generate(runner, date(2013, 9, 1), date_obj)

runner.enqueue_psql_script(get_rel_path(
    '../../spark_norm_common/lab_common_model.sql'
))

runner.enqueue_psql_script(get_rel_path('load_matching_payload.sql'), [
    ['matching_path', matching_path]
])

if args.period == 'current':
    runner.enqueue_psql_script(
        get_rel_path('load_and_merge_transactions.sql'), [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path]
        ]
    )
elif args.period == 'hist':
    runner.enqueue_psql_script(
        get_rel_path('load_transactions.sql'), [
            ['input_path', input_path]
        ]
    )
else:
    print("Invalid period '" + args.period + "'")
    exit(1)

runner.enqueue_psql_script(get_rel_path('normalize.sql'), [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '18'],
    ['vendor', '7']
])

# Privacy filtering
runner.enqueue_psql_script(
    get_rel_path('../../spark_norm_common/lab_post_normalization_cleanup.sql')
)

runner.execute_queue(args.debug)

runner.run_spark_script(
    get_rel_path('../../spark_norm_common/create_unload_lab_table.sql'),
    ['output_path', output_path]
)
runner.run_spark_script(
    get_rel_path('../../spark_norm_common/unload_common_model.sql'), [
        [
            'select_statement',
            "SELECT *, 'NULL' as magic_date "
            + "FROM lab_common_model "
            + "WHERE date_service is NULL",
            False
        ]
    ]
)
runner.run_spark_script(
    get_rel_path('../../spark_norm_common/unload_common_model.sql'), [
        [
            'select_statement',
            "SELECT *, regexp_replace(cast(date_service as string), '-..$', '') as magic_date "
            + "FROM lab_common_model "
            + "WHERE date_service IS NOT NULL",
            False
        ]
    ]
)

spark.sparkContext.stop()
