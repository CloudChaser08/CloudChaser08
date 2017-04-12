#! /usr/bin/python
import argparse
import time
from datetime import timedelta, datetime, date
from spark.runner import Runner
from spark.spark import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants

# init
spark, sqlContext = init("Quest")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

period = 'current' if date_obj.strftime('%Y%m%d') >= '20160831' \
         else 'hist'

setid = 'HealthVerity_' + \
        date_obj.strftime('%Y%m%d') + \
        (date_obj + timedelta(days=1)).strftime('%m%d')

script_path = __file__

input_path = 's3a://salusv/incoming/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
trunk_path = input_path + 'trunk/'
addon_path = input_path + 'addon/'

matching_path = 's3a://salusv/matching/payload/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)

# create helper tables
runner.run_spark_script(file_utils.get_rel_path(
    script_path,
    'create_helper_tables.sql'
))

runner.run_spark_script(file_utils.get_rel_path(
    script_path,
    '../../common/lab_common_model.sql'
), [
    ['table_name', 'lab_common_model', False],
    ['properties', '', False]
])

payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

if period == 'current':
    runner.run_spark_script(
        file_utils.get_rel_path(
            script_path, 'load_and_merge_transactions.sql'
        ), [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path]
        ]
    )
elif period == 'hist':
    runner.run_spark_script(
        file_utils.get_rel_path(
            script_path, 'load_transactions.sql'
        ), [
            ['input_path', input_path]
        ]
    )

runner.run_spark_script(file_utils.get_rel_path(
    script_path, 'normalize.sql'
), [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '18'],
    ['vendor', '7'],
    ['join', (
        'q.accn_id = mp.claimid AND mp.hvJoinKey = q.hv_join_key'
        if period == 'current' else 'q.accn_id = mp.claimid'
    ), False]
])

runner.run_spark_script(file_utils.get_rel_path(
    script_path,
    '../../common/lab_common_model.sql'
), [
    ['table_name', 'final_unload', False],
    [
        'properties',
        constants.unload_properties_template.format(args.output_path),
        False
    ]
])

runner.run_spark_script(
    file_utils.get_rel_path(
        script_path, '../../common/unload_common_model.sql'
    ), [
        [
            'select_statement',
            "SELECT *, 'quest' as provider, 'NULL' as best_date "
            + "FROM lab_common_model "
            + "WHERE date_service is NULL",
            False
        ],
        ['partitions', '20', False]
    ]
)
runner.run_spark_script(
    file_utils.get_rel_path(
        script_path, '../../common/unload_common_model.sql'
    ), [
        [
            'select_statement',
            "SELECT *, 'quest' as provider, "
            + "regexp_replace(cast(date_service as string), '-..$', '') as best_date "
            + "FROM lab_common_model "
            + "WHERE date_service IS NOT NULL",
            False
        ],
        ['partitions', '20', False]
    ]
)

spark.sparkContext.stop()
