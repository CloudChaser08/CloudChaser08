#! /usr/bin/python
import os
import argparse
import time
from datetime import datetime, date
from spark.runner import Runner
from spark.spark import init
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
spark, sqlContext = init("Practice Insight")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

setid = 'HV.data.837.' + str(date_obj.year) + '.csv.gz'

input_path = 's3a://salusv/incoming/medicalclaims/practice_insight/{}/'.format(
    str(date_obj.year)
)

output_path = 's3a://salusv/warehouse/text/medicalclaims/2017-02-24/part_provider=practice_insight/'

matching_path = 's3a://salusv/matching/payload/medicalclaims/practice_insight/{}/'.format(
    str(date_obj.year)
)

# create date and helper tables
date_validator.generate(runner, date(2013, 9, 1), date_obj.date())
runner.run_spark_script(get_rel_path('create_helper_tables.sql'))

runner.run_spark_script(get_rel_path(
    '../../common/medicalclaims_common_model.sql'
), [
    ['table_name', 'medicalclaims_common_model'],
    ['properties', '']
])

# load transactions and payload
runner.run_spark_script(get_rel_path('load_transactions.sql'), [
    ['input_path', input_path]
])
payload_loader.load(runner, matching_path, ['claimid'])

# create explosion maps
runner.run_spark_script(get_rel_path('create_exploded_diagnosis_map.sql'))
runner.run_spark_script(get_rel_path('create_exploded_procedure_map.sql'))

# normalize
runner.run_spark_script(get_rel_path('normalize.sql'), [
    ['setid', setid],
    ['today', TODAY],
    ['feedname', '22'],
    ['vendor', '3']
])

runner.run_spark_script(get_rel_path(
    '../../common/medicalclaims_common_model.sql'
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
            + "FROM final_unload "
            + "WHERE date_service is NULL",
            False
        ]
    ]
)
runner.run_spark_script(
    get_rel_path('../../common/unload_common_model.sql'), [
        [
            'select_statement',
            "SELECT *, regexp_replace(cast(date_service as string), '-..$', '') as best_date "
            + "FROM final_unload "
            + "WHERE date_service IS NOT NULL",
            False
        ]
    ]
)

spark.sparkContext.stop()
