#! /usr/bin/python
import os
import argparse
import time
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants


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

min_date = '2013-01-01'
max_date = args.date


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
dialysistreatment_input = input_prefix + 'dialysistreatment/'

matching_path = insert_date('s3://salusv/matching/payload/emr/visonex/{}/{}/{}/')

runner.run_spark_script('../../common/emr_common_model.sql', [
    ['table_name', 'emr_common_model', False],
    ['properties', '', False]
])

runner.run_spark_script('load_transactions.sql', [
    ['hospitalization_input', hospitalization_input],
    ['immunization_input', immunization_input],
    ['labpanelsdrawn_input', labpanelsdrawn_input],
    ['labresult_input', labresult_input],
    ['patientaccess_examproc_input', patientaccess_examproc_input],
    ['patientdiagcodes_input', patientdiagcodes_input],
    ['patientmedadministered_input', patientmedadministered_input],
    ['patientmedprescription_input', patientmedprescription_input],
    ['labidlist_input', labidlist_input],
    ['problemlist_input', problemlist_input],
    ['dialysistreatment_input', dialysistreatment_input]
])

payload_loader.load(runner, matching_path, ['claimId'])

runner.run_spark_script('normalize.sql', [
    ['today', TODAY],
    ['filename', 'HealthVerity-20170201'],
    ['feedname', '23'],
    ['vendor', '33'],
    ['min_date', min_date],
    ['max_date', max_date]
])

# correct record id
sqlContext.sql('select * from emr_common_model').withColumn(
    'record_id', monotonically_increasing_id()
).createTempView('emr_common_model')

runner.run_spark_script('../../common/emr_common_model.sql', [
    ['table_name', 'final_unload', False],
    [
        'properties',
        constants.unload_properties_template.format(args.output_path),
        False
    ]
])

runner.run_spark_script('../../common/unload_common_model.sql', [
    [
        'select_statement',
        "SELECT *, 'visonex' as provider, 'NULL' as best_date "
        + "FROM emr_common_model "
        + "WHERE date_start IS NULL",
        False
    ],
    ['partitions', '20', False],
    ['distribution_key', 'record_id', False]
])

runner.run_spark_script('../../common/unload_common_model.sql', [
    [
        'select_statement',
        "SELECT *, 'visonex' as provider, regexp_replace(cast(date_start as string), '-..$', '') as best_date "
        + "FROM emr_common_model "
        + "WHERE date_start IS NOT NULL",
        False
    ],
    ['partitions', '20', False],
    ['distribution_key', 'record_id', False]
])

spark.stop()
