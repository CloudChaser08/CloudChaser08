import os
import argparse
import time
from datetime import datetime, date
from spark.spark import init
from spark.runner import Runner
import spark.helpers.create_date_validation_table \
    as date_validator
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )


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

runner.run_spark_script(get_rel_path(
    '../../common/event_common_model.sql'
), [
    ['table_name', 'event_common_model', False],
    ['properties', '', False]
])

payload_loader.load(runner, matching_path, ['hvJoinKey', 'deathMonth'])

runner.run_spark_script(get_rel_path("load_transactions.sql"), [
    ['input_path', input_path]
])

runner.run_spark_script(get_rel_path("normalize.sql"), [
    ['set', 'obit'],
    ['feed', '27'],
    ['vendor', '49']
])

runner.run_spark_script(get_rel_path(
    '../../common/event_common_model.sql'
), [
    ['table_name', 'final_unload', False],
    [
        'properties',
        constants.unload_properties_template.format(output_path),
        False
    ]
])

runner.run_spark_script(
    get_rel_path('../../common/unload_common_model.sql'), [
        [
            'select_statement',
            "SELECT *, 'NULL' as best_date "
            + "FROM event_common_model "
            + "WHERE date_start IS NULL",
            False
        ]
    ]
)

runner.run_spark_script(
    get_rel_path('../../common/unload_common_model.sql'), [
        [
            'select_statement',
            "SELECT *, regexp_replace(cast(date_start as string), '-..$', '') as best_date "
            + "FROM event_common_model "
            + "WHERE date_start IS NOT NULL",
            False
        ]
    ]
)
