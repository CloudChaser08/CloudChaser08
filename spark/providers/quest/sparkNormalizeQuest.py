#! /usr/bin/python
import os
import argparse
import time
from datetime import timedelta, datetime, date
from spark.runner import Runner
from spark.spark import init
import spark.helpers.create_date_validation_table \
    as date_validator


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

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

period = 'current' if date_obj.strftime('%Y%m%d') >= '20160831' \
         else 'hist'

setid = 'HealthVerity_' + \
        date_obj.strftime('%Y%m%d') + \
        (date_obj + timedelta(days=1)).strftime('%m%d')

input_path = 's3a://salusv/incoming/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
trunk_path = input_path + 'trunk/'
addon_path = input_path + 'addon/'

matching_path = 's3a://salusv/matching/payload/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)



# create helper tables
runner.run_spark_script(get_rel_path(
    'create_helper_tables.sql'
))

# create date table
date_validator.generate(runner, date(2013, 9, 1), date_obj.date())

runner.run_spark_script(get_rel_path(
    '../../common/lab_common_model.sql'
))

runner.run_spark_script(get_rel_path('load_matching_payload.sql'), [
    ['matching_path', matching_path]
])

if period == 'current':
    runner.run_spark_script(
        get_rel_path('load_and_merge_transactions.sql'), [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path]
        ]
    )
elif period == 'hist':
    runner.run_spark_script(
        get_rel_path('load_transactions.sql'), [
            ['input_path', input_path]
        ]
    )

runner.run_spark_script(get_rel_path('normalize.sql'), [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '18'],
    ['vendor', '7'],
    ['join', (
        'q.accn_id = mp.claimid AND mp.hvJoinKey = q.hv_join_key'
        if period == 'current' else 'q.accn_id = mp.claimid'
    ), False]
])

# Privacy filtering
runner.run_spark_script(
    get_rel_path('../../common/lab_post_normalization_cleanup.sql')
)

runner.run_spark_script(
    get_rel_path('../../common/create_unload_lab_table.sql'), [
        ['output_path', args.output_path]
    ]
)
runner.run_spark_script(
    get_rel_path('../../common/unload_common_model.sql'), [
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
    get_rel_path('../../common/unload_common_model.sql'), [
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