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
output_path = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'


def run(spark, runner, date_input, test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    period = 'current' if date_obj.strftime('%Y%m%d') >= '20160831' \
             else 'hist'

    setid = 'HealthVerity_' + \
            date_obj.strftime('%Y%m%d') + \
            (date_obj + timedelta(days=1)).strftime('%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_rel_path(
            script_path, '../../test/providers/quest/resources/input/'
        ) + '/'
        matching_path = file_utils.get_rel_path(
            script_path, '../../test/providers/quest/resources/matching/'
        ) + '/'
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
    runner.run_spark_script(file_utils.get_rel_path(
        script_path,
        'create_helper_tables.sql'
    ))

    runner.run_spark_script(file_utils.get_rel_path(
        script_path,
        '../../common/lab_common_model.sql'
    ), [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    if period == 'current':
        runner.run_spark_script(
            file_utils.get_rel_path(
                script_path, 'load_and_merge_transactions.sql'
            ), [
                ['trunk_path', trunk_path],
                ['addon_path', addon_path]
            ]
        )
    elif period == 'hist':
        runner.run_spark_script(
            file_utils.get_rel_path(
                script_path, 'load_transactions.sql'
            ), [
                ['input_path', input_path]
            ]
        )
    else:
        logging.error('Invalid period')
        exit(1)

    runner.run_spark_script(file_utils.get_rel_path(
        script_path, 'normalize.sql'
    ), [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '18'],
        ['vendor', '7'],
        ['join', (
            'q.accn_id = mp.claimid AND mp.hvJoinKey = q.hv_join_key'
            if period == 'current' else 'q.accn_id = mp.claimid'
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

    run(spark, runner, args.date)

    spark.stop()

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
