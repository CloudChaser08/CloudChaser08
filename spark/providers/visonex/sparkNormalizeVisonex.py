#! /usr/bin/python
import os
import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            '/home/hadoop/spark/providers/visonex/',
            relative_filename
        )
    )


# init
spark, sqlContext = init("Visonex")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

# parser = argparse.ArgumentParser()
# parser.add_argument('--date', type=str)
# parser.add_argument('--output_path', type=str)
# parser.add_argument('--debug', default=False, action='store_true')
# args = parser.parse_args()
argdate = '2017-02-01'

date_obj = datetime.strptime(argdate, '%Y-%m-%d')


def insert_date(template):
    return template.format(
        str(date_obj.year),
        str(date_obj.month).zfill(2),
        str(date_obj.day).zfill(2)
    )


input_prefix = insert_date('s3://salusv/incoming/emr/visonex/{}/{}/{}/')
hospitalization_input = input_prefix + 'hospitalization/'
immunization_input = input_prefix + 'immunization/'
labpanelsdrawn_input = input_prefix + 'labpanelsdrawn/'
labresult_input = input_prefix + 'labresult/'
patientaccess_examproc_input = input_prefix + 'patientaccess_examproc/'
patientdiagcodes_input = input_prefix + 'patientdiagcodes/'
patientmedadministered_input = input_prefix + 'patientmedadministered/'
patientmedprescription_input = input_prefix + 'patientmedprescription/'
labidlist_input = input_prefix + 'labidlist/'
problemlist_input = input_prefix + 'problemlist/'

matching_path = insert_date('s3://salusv/matching/payload/emr/visonex/{}/{}/{}/')

output_path = 's3://salusv/warehouse/text/emr/2017-03-14/part_provider=visonex/'

runner.run_spark_script(get_rel_path(
    '../../common/emr_common_model.sql'
), [
    ['table_name', 'emr_common_model', False],
    ['properties', '', False]
])

runner.run_spark_script(
    get_rel_path('load_transactions.sql'), [
        ['hospitalization_input', hospitalization_input],
        ['immunization_input', immunization_input],
        ['labpanelsdrawn_input', labpanelsdrawn_input],
        ['labresult_input', labresult_input],
        ['patientaccess_examproc_input', patientaccess_examproc_input],
        ['patientdiagcodes_input', patientdiagcodes_input],
        ['patientmedadministered_input', patientmedadministered_input],
        ['patientmedprescription_input', patientmedprescription_input],
        ['labidlist_input', labidlist_input],
        ['problemlist_input', problemlist_input]
    ]
)

payload_loader.load(runner, matching_path, ['claimId'])

runner.run_spark_script(
    get_rel_path('normalize.sql'), [
        ['today', TODAY],
        ['filename', 'HealthVerity-20170201'],
        ['feedname', '23'],
        ['vendor', '33']
    ]
)


runner.run_spark_script(get_rel_path(
    '../../common/emr_common_model.sql'
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
            + "FROM emr_common_model "
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
            + "FROM emr_common_model "
            + "WHERE date_start IS NOT NULL",
            False
        ]
    ]
)

spark.stop()
