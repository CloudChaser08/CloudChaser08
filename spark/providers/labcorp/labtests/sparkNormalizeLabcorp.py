#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.labtests as lab_priv


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/labcorp/labtests/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/labcorp/labtests/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/labcorp/labtests/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/labcorp/labtests/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/labtests/labcorp/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/labcorp/{}/'.format(
            date_input.replace('-', '/')
        )

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    vendor_feed_id = '46'
    vendor_id = '10'

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    hvm_available_history_date = postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "HVM_AVAILABLE_HISTORY_START_DATE")
    earliest_valid_service_date = postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else datetime.date(1901, 1, 1)
    max_date = date_input

    runner.run_spark_script('../../../common/lab_common_model_v3.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    # trim and remove nulls from raw input
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from transactions')
    ).createTempView('transactions')

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('normalize.sql')

    postprocessor.compose(
        postprocessor.add_universal_columns(
            feed_id=vendor_feed_id,
            vendor_id=vendor_id,
            filename='record_data_HV_{}.txt.name.csv'.format(date_obj.strftime('%Y%m%d'))
        ),
        lab_priv.filter(runner.sqlContext),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_specimen', max_date, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE')
    )(
        runner.sqlContext.sql('select * from lab_common_model')
    ).createTempView('lab_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'labtests', 'lab_common_model_v3.sql', 'labcorp',
            'lab_common_model', 'date_specimen', date_input,
            hvm_historical_date=datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )


def main(args):

    # init spark
    spark, sqlContext = init("Labcorp")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/labcorp/labtests/spark-output/'
    else:
        output_path = 's3a://salusv/warehouse/parquet/labtests/2017-02-16/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
