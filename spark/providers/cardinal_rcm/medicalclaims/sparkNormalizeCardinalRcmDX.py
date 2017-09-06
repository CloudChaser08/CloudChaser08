#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_rcm/medicalclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_rcm/medicalclaims/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_rcm/medicalclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_rcm/medicalclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/medicalclaims/cardinal_rcm/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/medicalclaims/cardinal_rcm/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2010-03-01'
    max_date = date_input

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    runner.run_spark_script('../../../common/medicalclaims_common_model.sql', [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    explode.generate_exploder_table(spark, 9, 'diag_exploder')
    explode.generate_exploder_table(spark, 2, 'proc_exploder')

    # trim and remove nulls from raw input
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from transactions')
    ).createTempView('transactions')

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('normalize_service_lines.sql')

    vendor_feed_id = '29'
    vendor_id = '42'

    postprocessor.compose(
        postprocessor.add_universal_columns(
            feed_id=vendor_feed_id,
            vendor_id=vendor_id,

            # TODO: this is incorrect - fix when we find out what
            # their filenames will be named
            filename='RCM_Claims_{}.open'.format(date_obj.strftime('%Y%m%d'))
        ),
        medical_priv.filter,
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service', date_input, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE'),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service_end', date_input, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE')
    )(
        runner.sqlContext.sql('select * from medicalclaims_common_model')
    ).createTempView('medicalclaims_common_model')

    explode.explode_medicalclaims_dates(runner)

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'cardinal_rcm',
            'medicalclaims_common_model', 'date_service', date_input
        )


def main(args):

    # init spark
    spark, sqlContext = init("Cardinal RCM")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_rcm/medicalclaims/spark-output/'
    else:
        output_path = 's3a://salusv/warehouse/parquet/medicalclaims/2017-02-24/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
