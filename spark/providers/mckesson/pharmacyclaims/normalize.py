#! /usr/bin/python
import argparse
from datetime import datetime
from pyspark.sql.functions import lit, col
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims import schemas as pharma_schemas
import spark.helpers.file_utils as file_utils
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv
import spark.providers.mckesson.pharmacyclaims.transaction_schemas as transaction_schemas

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


runner = None
spark = None
pharma_schema = pharma_schemas['schema_v6']

OUTPUT_PATH_PRODUCTION = 's3a://salusv/warehouse/parquet/' + pharma_schema.output_directory
OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/spark-output/'


def load(input_path, restriction_level, runner):
    """
    Function for loading transactional data.

    Mckesson has 2 schemas - their old schema had 119 columns, their
    new schema has 118. There is no date cutoff for when the new
    schema starts and the old ends, so the best way to choose the
    correct schema is to simply count the columns.
    """

    unlabeled_input = runner.sqlContext.read.csv(input_path, sep='|')

    if len(unlabeled_input.columns) == 118:
        labeled_input = runner.sqlContext.createDataFrame(
            unlabeled_input.rdd, transaction_schemas.new_schema
        ).withColumn(
            'DispensingFeePaid', lit(None)
        ).withColumn(
            'FillerTransactionTime', col('ClaimTransactionTime')
        )
    elif len(unlabeled_input.columns) == 119:
        labeled_input = runner.sqlContext.createDataFrame(unlabeled_input.rdd, transaction_schemas.old_schema)
    else:
        raise ValueError('Unexpected column length in transaction data: {}'.format(str(len(unlabeled_input.columns))))

    postprocessor.compose(
        postprocessor.trimmify, postprocessor.nullify
    )(labeled_input).createOrReplaceTempView('{}_transactions'.format(restriction_level))


def postprocess_and_unload(date_input, test_dir, runner):
    """
    Function for unloading normalized data
    """
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid_template = '{}.Record.' + date_obj.strftime('%Y%m%d')
    setid = setid_template.format('HVUnRes')
    provider = 'mckesson'
    restriction_level = 'unrestricted'
    feed_id = '33'
    vendor_id = '86'

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id=feed_id, vendor_id=vendor_id, filename=setid),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from {}_pharmacyclaims_common_model'.format(restriction_level))
    ).createOrReplaceTempView('{}_pharmacyclaims_common_model'.format(restriction_level))

    normalized_records_unloader.partition_and_rename(
        spark, runner, 'pharmacyclaims', 'pharmacyclaims/sql/pharmacyclaims_common_model_v6.sql', provider,
        '{}_pharmacyclaims_common_model'.format(restriction_level), 'date_service', date_input,
        test_dir=test_dir
    )


def run(spark_in, runner_in, date_input, test=False, airflow_test=False):
    script_path = __file__

    global spark, runner
    spark = spark_in
    runner = runner_in

    if test:
        unres_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/input/'
        ) + '/'
        unres_matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/matching/'
        ) + '/'
  
    elif airflow_test:
        unres_input_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_matching_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )

    else:
        unres_input_path = 's3a://salusv/incoming/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )    
        unres_matching_path = 's3a://salusv/matching/payload/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2010-03-01'
    max_date = date_input

    def normalize(input_path, matching_path):
        """
        Generic function for running normalization in any mode
        """
        restriction_level = 'unrestricted'

        load(input_path, restriction_level, runner)

        payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

        normalized_output = runner.run_spark_script('normalize.sql', [
            ['min_date', min_date],
            ['max_date', max_date],
            ['restriction_level', restriction_level, False]
        ], return_output=True)

        schema_enforcer.apply_schema(
            normalized_output, pharma_schema.schema_structure
        ).createOrReplaceTempView('{}_pharmacyclaims_common_model'.format(restriction_level))

        test_dir = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/output/'
        ) + '/' if test else None

        postprocess_and_unload(date_input, test_dir, runner)

    # run normalization for appropriate mode(s)
    normalize(unres_input_path, unres_matching_path)

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='McKessonRx',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=unres_input_path,
            data_source_matching_path=unres_matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):

    # init spark
    spark, sql_context = init("McKessonRx")

    # initialize runner
    runner = Runner(sql_context)
    
    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
