#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import udf, lit
import transactions
import spark.helpers.schema_enforcer as schema_enforcer

def run(spark, runner, date_input, pharmacy_name, test=False, airflow_test=False):
    if test:
        incoming_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/liquidhub/custom/resources/incoming/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/liquidhub/custom/resources/matching/'
        ) + '/'
        output_dir = '/tmp/staging/' + date_input.replace('-', '/') + '/'
    elif airflow_test:
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/lhv2/custom/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/lhv2/custom/incoming/{}/'.format(
            date_input.replace('-', '/')
        )
        output_dir = '/tmp/staging/' + date_input.replace('-', '/') + '/'
    else:
        incoming_path = 's3a://salusv/incoming/custom/lhv2/{}/{}/'.format(
            pharmacy_name, date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/custom/lhv2/{}/{}/'.format(
            pharmacy_name, date_input.replace('-', '/')
        )
        output_dir = constants.hdfs_staging_dir + date_input.replace('-', '/') + '/'

    payload_loader.load(runner, matching_path, ['claimId', 'topCandidates', 'matchStatus', 'hvJoinKey', 'isWeak', 'providerMatchId'])
    records_loader.load_and_clean_all(runner, incoming_path, transactions, 'csv', '|')

    content = runner.run_spark_script('normalize.sql', [
        ['location', output_dir]
    ], return_output=True)

    schema = StructType([
            StructField('hvid', StringType(), True),
            StructField('lhid', StringType(), True),
            StructField('pharmacy_name', StringType(), True),
            StructField('brand', StringType(), True),
        ] + [
            StructField('filler_' + str(i), StringType(), True) for i in xrange(1, 12)
        ] + [
            StructField('weak_match', StringType(), True),
            StructField('provider_specific_id', StringType(), True),
            StructField('provider_meta', StringType(), True),
            StructField('matching_meta_data', StringType(), True)
        ]
    )

    content = schema_enforcer.apply_schema(content, schema)
    header = spark.createDataFrame(
        [tuple(
            ['HVID', 'LHID', 'Pharmacy Name', 'Brand'] +
            ['filler_' + str(i) for i in xrange(1, 12)] +
            ['Weak Match', 'Provider Specific ID', 'Provider Meta', 'Matching Meta Data'])],
        schema=schema
    )
    deliverable = header.union(content).coalesce(1)

    deliverable.createOrReplaceTempView('liquidhub_deliverable')
    if test:
        deliverable.createOrReplaceTempView('liquidhub_deliverable')
    else:
        normalized_records_unloader.unload_delimited_file(
            spark, runner, '/staging/' + pharmacy_name + '/' + date_input.replace('-', '/') + '/', 'liquidhub_deliverable',
            file_name='LH_Amgen_' + pharmacy_name + '_' + date_input.replace('-', '') + '_FILEID.txt.gz')

def main(args):
    # init
    spark, sqlContext = init("Liquidhub Mastering")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, args.pharmacy_name, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/spark-output/'
    else:
        output_path = 's3a://salusv/deliverable/lhv2/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--pharmacy_name', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
