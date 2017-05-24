#! /usr/bin/python
import argparse
import time
import logging
from datetime import timedelta, datetime
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.udf

TODAY = time.strftime('%Y-%m-%d', time.localtime())
output_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2017-05-24/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'HVUnRes.Record.' + date_obj.strftime('%Y%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/mckesson/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/mckesson/pharmacyclaims/matching/'
        ) + '/'

    min_date = '1900-01-01'
    max_date = date_input

    # create helper tables
    runner.run_spark_script('create_helper_tables.sql')

    runner.run_spark_script('../../common/pharmacyclaims_common_model.sql', [
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    runner.run_spark_script('normalize.sql', [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '33'],
        ['vendor', '86'],
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'lab', 'pharmacyclaims_common_model.sql', 'mckesson',
            'pharmacyclaims_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("McKessonRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
