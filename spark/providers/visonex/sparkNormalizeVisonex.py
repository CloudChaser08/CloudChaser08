import argparse
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader

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
matching_path = insert_date('s3://salusv/matching/payload/emr/visonex/{}/{}/{}/')

runner.run_spark_script('../../common/emr_common_model.sql', [
    ['table_name', 'emr_common_model', False],
    ['properties', '', False]
])

runner.run_spark_script('load_transactions.sql', [
    ['input_path', input_prefix, False]
])

payload_loader.load(runner, matching_path, ['claimId'])

runner.run_spark_script('normalize.sql', [
    ['today', TODAY],
    ['filename', 'HealthVerity-{}-{}.zip'.format(
        date_obj.strftime('%Y%m01'), ((date_obj.replace(day=1) + relativedelta(months=1))
                                      - relativedelta(days=1)).strftime('%Y%m%d')
    )],
    ['feedname', '23'],
    ['vendor', '33'],
    ['min_date', min_date],
    ['max_date', max_date]
])

# correct record id
sqlContext.sql('select * from emr_common_model').withColumn(
    'record_id', monotonically_increasing_id()
).createTempView('emr_common_model')

normalized_records_unloader.partition_and_rename(
    spark, runner, 'emr', 'emr_common_model.sql', 'visonex',
    'emr_common_model', 'date_start', args.date
)
normalized_records_unloader.distcp(args.output_path)

spark.stop()
