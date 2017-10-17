#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, lit

def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    script_path = __file__

    if test:
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_mpi/custom/resources/matching/'
        ) + '/'
        output_dir = '/tmp/staging/' + date_input.replace('-', '/') + '/'
    elif airflow_test:
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        output_dir = '/tmp/staging/' + date_input.replace('-', '/') + '/'
    else:
        matching_path = 's3a://salusv/matching/payload/custom/cardinal_mpi/{}/'.format(
            date_input.replace('-', '/')
        )
        output_dir = constants.hdfs_staging_dir + date_input.replace('-', '/') + '/'

    payload_loader.load(runner, matching_path, ['claimId', 'topCandidates', 'matchStatus'])

    # Bug in Spark that prevents querying the matching_payload table directly
    # from Spark. Create a copy of the table so we can query it.
    # https://issues.apache.org/jira/browse/SPARK-18589
    runner.sqlContext.sql('DROP TABLE IF EXISTS matching_payload_count')
    runner.sqlContext.sql('CREATE TABLE matching_payload_count AS SELECT * FROM matching_payload')

    # topCandidates is suppose to be a column of array type. If all the values
    # are NULL it ends up being a string type. Replace it with an array type
    # column of all nulls so the routine doesn't break
    if runner.sqlContext.sql('SELECT * FROM matching_payload_count WHERE topCandidates IS NOT NULL').count() == 0:
        null_array_column = udf(lambda x: None, ArrayType(ArrayType(StringType(), True), True))(lit(None))
        runner.sqlContext.sql('SELECT * FROM matching_payload') \
            .withColumn('topCandidates', null_array_column) \
            .createOrReplaceTempView("matching_payload") \

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
