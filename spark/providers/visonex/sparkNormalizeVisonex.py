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
            '/home/hadoop/spark/providers/visonex/',
            relative_filename
        )
    )


# init
spark, sqlContext = init("Visonex")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

input_prefix = 's3://salusv/incoming/emr/visonex/{}/{}/{}/'.format(
    str(date_obj.year),
    str(date_obj.month).zfill(2),
    str(date_obj.day).zfill(2)
)
hospitalization_input = input_prefix + 'hospitalization/'
immunization_input = input_prefix + 'immunization/'
labpanelsdrawn_input = input_prefix + 'labpanelsdrawn/'
labresult_input = input_prefix + 'labresult/'
patientaccess_examproc_input = input_prefix + 'patientaccess_examproc/'
patientdiagcodes_input = input_prefix + 'patientdiagcodes/'
patientmedadministered_input = input_prefix + 'patientmedadministered/'
labidlist_input = input_prefix + 'labidlist/'

runner.run_spark_script(
    get_rel_path('load_transactions.sql'), [
        ['hospitalization_input', hospitalization_input],
        ['immunization_input', immunization_input],
        ['labpanelsdrawn_input', labpanelsdrawn_input],
        ['labresult_input', labresult_input],
        ['patientaccess_examproc_input', patientaccess_examproc_input],
        ['patientdiagcodes_input', patientdiagcodes_input],
        ['patientmedadministered_input', patientmedadministered_input],
        ['labidlist_input', labidlist_input]
    ]
)

spark.sparkContext.stop()
