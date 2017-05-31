#! /usr/bin/python
import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

TODAY = time.strftime('%Y-%m-%d', time.localtime())


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'HVUnRes.Record.' + date_obj.strftime('%Y%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/matching/'
        ) + '/'
    else:
        input_path = 's3a://healthveritydev/musifer/norm/mckessonrx/transaction/'
        matching_path = 's3a://healthveritydev/musifer/norm/mckessonrx/matching/'

    min_date = '1900-01-01'
    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims_common_model.sql', [
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False]
    ], source_file_path=script_path)

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ], source_file_path=script_path)

    runner.run_spark_script('normalize.sql', [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '33'],
        ['vendor', '86'],
        ['min_date', min_date],
        ['max_date', max_date]
    ], source_file_path=script_path)

    pharm_priv.filter(
        postprocessor.nullify(runner.sqlContext.sql('select * from pharmacyclaims_common_model'))
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model.sql', 'mckesson',
            'pharmacyclaims_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("McKessonRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    output_path = 's3a://salusv/testing/mckessonrx/sample-out/'
    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
