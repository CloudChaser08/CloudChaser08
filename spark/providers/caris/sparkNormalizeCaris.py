#! /usr/bin/python
import argparse
from datetime import datetime
import calendar

import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as explode
import spark.helpers.file_utils as file_utils
import spark.helpers.postprocessor as postprocessor
from spark.spark_setup import init
from spark.runner import Runner

AIRFLOW_E2E_BASE = 's3://salusv/testing/dewey/airflow/e2e/caris/labtests/'


def run(spark, runner, date_input, test=False, airflow_test=False):

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/caris/resources/input/raw/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/caris/resources/matching/'
        ) + '/'
        addon_path = file_utils.get_abs_path(
            script_path, '../../test/providers/caris/resources/input/addon/'
        ) + '/'

    elif airflow_test:
        input_path = AIRFLOW_E2E_BASE + 'out/{year}/{month}/'.format(
            year=str(date_obj.year),
            month=str(date_obj.month).zfill(2)
        )
        matching_path = AIRFLOW_E2E_BASE + 'payload/{year}/{month}/'.format(
            year=str(date_obj.year),
            month=str(date_obj.month).zfill(2)
        )
        addon_path = 's3a://salusv/incoming/labtests/caris/hist_additional_columns/'

    else:
        input_path = 's3a://salusv/incoming/labtests/caris/{}/{:02d}/'.format(date_obj.year, date_obj.month)
        matching_path = 's3a://salusv/matching/payload/labtests/caris/{}/{:02d}/'.format(date_obj.year, date_obj.month)
        addon_path = 's3a://salusv/incoming/labtests/caris/hist_additional_columns/'

    setid = 'Data_7_29' if date_input == '2016-08-01' \
            else 'DATA_{}{:02d}01'.format(date_obj.year, date_obj.month)

    min_date = '2005-01-01'

    # max cap at the last day of the month
    max_date = date_obj.strftime('%Y-%m-') \
        + str(calendar.monthrange(date_obj.year, date_obj.month)[1])

    explode.generate_exploder_table(spark, 200)
    runner.run_spark_script('../../common/zip3_to_state.sql')

    # create helper tables
    runner.run_spark_script('create_helper_tables.sql')

    runner.run_spark_script('../../common/lab_common_model.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    # append additional columns
    if date_input <= '2017-04-01':
        runner.run_spark_script('load_transactions_legacy.sql', [
            ['input_path', input_path],
            ['addon_path', addon_path]
        ])
    else:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path]
        ])

    runner.run_spark_script('normalize.sql', [
        ['date_received', date_input],
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    postprocessor.add_universal_columns('14', '13', setid)(
        runner.sqlContext.sql('select * from lab_common_model')
    ).createTempView('lab_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'lab', 'lab_common_model.sql', 'caris', 'lab_common_model',
            'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("Caris")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = AIRFLOW_E2E_BASE + 'spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
