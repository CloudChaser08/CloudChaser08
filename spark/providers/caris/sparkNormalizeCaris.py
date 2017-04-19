#! /usr/bin/python
import argparse
import time
from datetime import datetime
import calendar
from pyspark.sql.functions import monotonically_increasing_id, lit

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.spark import init
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
addon_path = 's3a://salusv/incoming/labtests/caris/hist_additional_columns/'

script_path = __file__

setid = 'DATA_' + str(date_obj.year) \
        + str(date_obj.month).zfill(2) + '01' \
        if args.date != '2016-08-01' else 'Data_7_29'

min_date = '2005-01-01'
max_date = date_obj.strftime('%Y-%m-') \
           + str(calendar.monthrange(date_obj.year, date_obj.month)[1])

staging_dir = 'hdfs:///text-out/'

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

# append additional columns
if args.date <= '2017-04-01':
    runner.run_spark_script(file_utils.get_rel_path(
        script_path, 'load_additional_columns.sql'
    ), [['addon_path', addon_path]])

    sqlContext.sql("""
    SELECT t.*, a.ods_id as ods_id,
    a.accession_date AS accession_date,
    a.sign_out_date AS sign_out_date
    FROM raw_transactional t
    LEFT JOIN additional_columns a
    ON t.customer__patient_id = a.patient_id
    """).createTempView('raw_transactional')
else:
    # TODO: Adjust for new model that already contains these columns
    sqlContext.sql('select * from raw_transactional')  \
              .withColumn('ods_id', lit(None))              \
              .withColumn('accession_date', lit(None))      \
              .withColumn('sign_out_date', lit(None))       \
              .createTempView('raw_transactional')

runner.run_spark_script(
    file_utils.get_rel_path(script_path, 'normalize.sql'), [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '14'],
        ['vendor', '13'],
        ['date_received', args.date],
        ['min_date', min_date],
        ['max_date', max_date]
    ]
)

sqlContext.sql('select * from lab_common_model').withColumn(
    'record_id', monotonically_increasing_id()
).createTempView('lab_common_model')

normalized_records_unloader.unload(
    spark, runner, 'lab', 'caris', 'date_service', args.date, args.output_path
)

spark.stop()
