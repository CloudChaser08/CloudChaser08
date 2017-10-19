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

TODAY = time.strftime('%Y-%m-%d', time.localtime())


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'HealthVerity_' + \
            date_obj.strftime('%Y%m%d') + \
            (date_obj + timedelta(days=1)).strftime('%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/quest/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/quest/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/labtests/quest/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/quest/{}/'.format(
            date_input.replace('-', '/')
        )

    trunk_path = input_path + 'trunk/'
    addon_path = input_path + 'addon/'

    min_date = '2013-01-01'
    max_date = date_input

    # create helper tables
    runner.run_spark_script('create_helper_tables.sql')

    runner.run_spark_script('../../common/lab_common_model.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    if date_obj.strftime('%Y%m%d') >= '20171016':
        runner.run_spark_script('load_and_merge_transactions_v2.sql', [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path]
        ])
    elif date_obj.strftime('%Y%m%d') >= '20160831':
        runner.run_spark_script('load_and_merge_transactions.sql', [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path]
        ])
    else:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path]
        ])

    runner.run_spark_script('normalize.sql', [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '18'],
        ['vendor', '7'],
        ['join', (
            'q.accn_id = mp.claimid AND mp.hvJoinKey = q.hv_join_key'
            if date_obj.strftime('%Y%m%d') >= '20160831' else 'q.accn_id = mp.claimid'
        ), False],
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    # add in primary key
    runner.sqlContext.sql('select * from lab_common_model').withColumn(
        'record_id', monotonically_increasing_id()
    ).createTempView(
        'lab_common_model'
    )

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'lab', 'lab_common_model.sql', 'quest',
            'lab_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("Quest")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
