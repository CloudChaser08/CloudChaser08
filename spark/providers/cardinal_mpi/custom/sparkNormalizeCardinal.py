#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    script_path = __file__

    if test:
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_mpi/custom/resources/matching/'
        ) + '/'
        output_dir = '/tmp/staging/' + date_input.replace('-', '/') + '/'
    elif airflow_test:
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        output_dir = '/tmp/staging/' + date_input.replace('-', '/') + '/'
    else:
        matching_path = 's3a://salusv/matching/payload/custom/cardinal_mpi/{}/'.format(
            date_input.replace('-', '/')
        )
        output_dir = constants.hdfs_staging_dir + date_input.replace('-', '/') + '/'

    payload_loader.load(runner, matching_path, ['claimId', 'multiMatchQuality'])

    runner.run_spark_script('normalize.sql', [
        ['location', output_dir]
    ])


def main(args):
    # init
    spark, sqlContext = init("CardinalRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/spark-output/'
    else:
        output_path = 's3a://salusv/warehouse/text/custom/cardinal_mpi/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
