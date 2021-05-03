#! /usr/bin/python
import argparse
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, lit

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3a://salusv/deliverable/cardinal_mpi-0/'


def run(spark, runner, batch_id, test=False, airflow_test=False):

    script_path = __file__

    if test:
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_mpi/custom/resources/matching/'
        ) + '/'
        output_dir = '/tmp/staging/' + batch_id + '/'
    elif airflow_test:
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/payload/{}/'.format(
            batch_id
        )
        output_dir = '/tmp/staging/' + batch_id + '/'
    else:
        matching_path = 's3a://salusv/matching/payload/custom/cardinal_mpi/{}/'.format(
            batch_id
        )
        output_dir = constants.hdfs_staging_dir + batch_id + '/'

    payload_loader.load(runner, matching_path, ['claimId', 'topCandidates', 'matchStatus'])

    # Bug in Spark that prevents querying the matching_payload table directly
    # from Spark. Create a copy of the table so we can query it.
    # https://issues.apache.org/jira/browse/SPARK-18589
    runner.sqlContext.sql('DROP TABLE IF EXISTS matching_payload_count')
    runner.sqlContext.sql('CREATE TABLE matching_payload_count AS SELECT * FROM matching_payload')

    # topCandidates is suppose to be a column of array type. If all the values
    # are NULL it ends up being a string type. Replace it with an array type
    # column of all nulls so the routine doesn't break
    if runner.sqlContext.sql('SELECT * FROM matching_payload_count WHERE topcandidates IS NOT NULL').count() == 0:
        null_array_column = udf(lambda x: None, ArrayType(ArrayType(StringType(), True), True))(lit(None))
        runner.sqlContext.sql('SELECT * FROM matching_payload') \
            .withColumn('topcandidates', null_array_column) \
            .createOrReplaceTempView("matching_payload")

    runner.run_spark_script('normalize.sql', [
        ['location', output_dir]
    ])

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='CardinalRx',
            data_type=DataType.CUSTOM,
            data_source_transaction_path="",
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=batch_id
        )


def main(args):
    if args.date is not None:
        batch_id = args.date.replace('-', '/')
    else:
        batch_id = args.batch_id
    # init
    spark, sql_context = init("CardinalRx")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, batch_id, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch_id', type=str)
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()
    main(args)
