#! /usr/bin/python
import os
import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants
import spark.helpers.explode as explode


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )


# init
spark, sqlContext = init("Practice Insight")

# Set shuffle partitions to stabilize job
#
# This number is currently based on an assumption that this script
# will run on 5 m4.2xlarge nodes
sqlContext.setConf("spark.sql.shuffle.partitions", "1200")

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

input_path = 's3a://salusv/incoming/medicalclaims/practice_insight_split_month/{}/{}/'.format(
    str(date_obj.year),
    str(date_obj.month).zfill(2)
)

matching_path = 's3a://salusv/matching/payload/medicalclaims/practice_insight/{}/'.format(
    str(date_obj.year)
)


def run(part):
    # create helper tables
    runner.run_spark_script(get_rel_path('create_helper_tables.sql'))

    runner.run_spark_script(get_rel_path(
        '../../common/medicalclaims_common_model.sql'
    ), [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])

    # load transactions and payload
    runner.run_spark_script(get_rel_path('load_transactions.sql'), [
        ['input_path', input_path + part + '/']
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

    # explode date ranges
    explode.explode_dates(
        runner, 'medicalclaims_common_model', 'date_service',
        'date_service_end', 'record_id'
    )

    # unload
    runner.run_spark_script(get_rel_path(
        '../../common/medicalclaims_common_model.sql'
    ), [
        ['table_name', 'final_unload', False],
        [
            'properties',
            constants.unload_properties_template.format(args.output_path + part),
            False
        ]
    ])

    runner.run_spark_script(
        get_rel_path('../../common/unload_common_model.sql'), [
            [
                'select_statement',
                "SELECT *, 'practice_insight' as provider, 'NULL' as best_date "
                + "FROM medicalclaims_common_model "
                + "WHERE date_service is NULL",
                False
            ]
        ]
    )
    runner.run_spark_script(
        get_rel_path('../../common/unload_common_model.sql'), [
            [
                'select_statement',
                "SELECT *, 'practice_insight' as provider, regexp_replace("
                + "cast(date_service as string), "
                + "'-..$', '') as best_date "
                + "FROM medicalclaims_common_model "
                + "WHERE date_service IS NOT NULL",
                False
            ]
        ]
    )

    spark.catalog.clearCache()


for part in ['1', '2']:
    run(part)

spark.sparkContext.stop()
