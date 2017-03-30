#! /usr/bin/python
import argparse
import time
from datetime import datetime
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
from spark.spark import init
import spark.helpers.constants as constants
from spark.runner import Runner

# init
spark, sqlContext = init("Caris")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

input_path = 's3a://salusv/incoming/labtests/caris/{year}/{month}/'.format(
    year=str(date_obj.year),
    month=str(date_obj.month).zfill(2)
)
matching_path = 's3a://salusv/matching/payload/labtests/caris/{year}/{month}/'.format(
    year=str(date_obj.year),
    month=str(date_obj.month).zfill(2)
)
# script_path = __file__
script_path = '/home/hadoop/spark/providers/caris/sparkNormalizeCaris.py'

setid = 'DATA_' + str(date_obj.year) \
        + str(date_obj.month).zfill(2) + '01'


runner.run_spark_script(file_utils.get_rel_path(
    script_path, '../../common/zip3_to_state.sql'
))

runner.run_spark_script(file_utils.get_rel_path(
    script_path,
    '../../common/lab_common_model.sql'
), [
    ['table_name', 'lab_common_model', False],
    ['properties', '', False]
])

payload_loader.load(runner, matching_path, ['hvJoinKey'])

runner.run_spark_script(
    file_utils.get_rel_path(script_path, 'load_transactions.sql'), [
        ['input_path', input_path]
    ]
)

runner.run_spark_script(file_utils.get_rel_path(script_path, 'normalize.sql'), [
    ['filename', setid],
    ['today', TODAY],
    ['feedname', '14'],
    ['vendor', '13'],
    ['date_received', args.date]
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
            "SELECT *, 'caris' as provider, 'NULL' as best_date "
            + "FROM lab_common_model "
            + "WHERE date_service IS NULL",
            False
        ]
    ]
)

runner.run_spark_script(
    file_utils.get_rel_path(
        script_path, '../../common/unload_common_model.sql'
    ), [
        [
            'select_statement',
            "SELECT *, 'caris' as provider, regexp_replace(cast(date_service as string), '-..$', '') as best_date "
            + "FROM lab_common_model "
            + "WHERE date_service IS NOT NULL",
            False
        ]
    ]
)

spark.stop()
