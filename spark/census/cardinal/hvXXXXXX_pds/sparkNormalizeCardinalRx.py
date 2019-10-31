#! /usr/bin/python
import argparse
import time
from datetime import datetime, timedelta
import subprocess
from pyspark.sql.functions import lit, md5, col
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims_common_model_v6 import schema as pharma_schema
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


TEXT_FORMAT = """
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
"""
PARQUET_FORMAT = "STORED AS PARQUET"
EXTRA_COLUMNS = ['tenant_id', 'hvm_approved']

OUTPUT_PATH_TEST = \
    's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/spark-deliverable-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/deliverable/cardinal_pds-0/'
DELIVERABLE_HDFS_LOC = 'hdfs:///cardinal_pds_deliverable/'


def run(spark, runner, date_input, batch_id, test=False, airflow_test=False):
    org_num_partitions = spark.conf.get('spark.sql.shuffle.partitions')
    setid = 'PDS.' + batch_id.replace('/', '-')
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_pds/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_pds/pharmacyclaims/resources/matching/'
        ) + '/'
        deliverable_path = '/tmp/cardinal_pds_deliverable/'
    elif airflow_test:
        input_path = \
            's3a://salusv/incoming/pharmacyclaims/cardinal_pds/{}/'.format(batch_id)
        matching_path = \
            's3a://salusv/matching/payload/pharmacyclaims/cardinal_pds/{}/'.format(batch_id)
        deliverable_path = DELIVERABLE_HDFS_LOC
    else:
        input_path = \
            's3a://salusv/incoming/pharmacyclaims/cardinal_pds/{}/'.format(batch_id)
        matching_path = \
            's3a://salusv/matching/payload/pharmacyclaims/cardinal_pds/{}/'.format(batch_id)
        deliverable_path = DELIVERABLE_HDFS_LOC

    min_date = '2011-01-01'
    max_date = date_input

    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    column_length = len(spark.read.csv(input_path, sep='|').columns)
    if column_length == 86:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path]
        ])
    else:
        runner.run_spark_script('load_transactions_v2.sql', [
            ['input_path', input_path]
        ])

    df = runner.sqlContext.sql('select * from transactions')
    df = postprocessor.nullify(df, ['NULL', 'Unknown', '-1', '-2'])
    postprocessor.trimmify(df).createTempView('transactions')

    normalized_output = runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)

    postprocessor.compose(
        schema_enforcer.apply_schema_func(pharma_schema, cols_to_keep=EXTRA_COLUMNS),
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='39', vendor_id='42', filename=setid),
        pharm_priv.filter
    )(
        normalized_output
    ).persist().createTempView('pharmacyclaims_common_model')

    spark.table('pharmacyclaims_common_model').count()

    properties = ""

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v6.sql', [
        ['external', '', False],
        ['additional_columns', [], False],
        ['table_name', 'normalized_claims', False],
        ['properties', properties, False]
    ])

    if test:
        subprocess.check_call(['rm', '-rf', deliverable_path])
    else:
        subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', deliverable_path])

    tbl = spark.table('pharmacyclaims_common_model')
    (
        tbl.select(*[col(c).cast('string').alias(c) for c in tbl.columns])
            .createOrReplaceTempView('pharmacyclaims_common_model_strings')
    )

    # Create deliverable for Cardinal including all rows
    runner.run_spark_script('create_cardinal_deliverable.sql', [
        ['location', deliverable_path],
        ['partitions', org_num_partitions, False]
    ])

    # capture run details
    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='CardinalRx',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.CENSUS,
            input_date=date_input
        )


def main(args):
    # init
    spark, sqlContext = init("CardinalRx")

    # initialize runner
    runner = Runner(sqlContext)

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    run(spark, runner, date_input, batch_id, airflow_test=args.airflow_test)
    spark.stop()

    start = time.time()
    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        DELIVERABLE_HDFS_LOC, '--dest', output_path + args.date.replace('-', '/') + '/'
    ])
    dist_time = time.time() - start

    # write the run details to s3
    if not args.airflow_test:
        RunRecorder().record_run_details(additional_time=dist_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch_id', type=str)
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()

    # date input is used for max_date in the normalization - it is required
    if args.date is None:
        raise ValueError("This normalization requires a date argument")
    else:
        date_input = args.date
    # batch_id isn't required, but if it is not provided, will default to date_input
    if args.batch_id is None:
        batch_id = date_input.replace('-', '/')
    else:
        batch_id = args.batch_id

    main(args)
