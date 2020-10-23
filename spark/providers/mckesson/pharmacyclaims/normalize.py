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

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3a://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/'


def load(input_path, restriction_level):
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


def postprocess_and_unload(date_input, restricted, test_dir):
    """
    Function for unloading normalized data
    """
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid_template = '{}.Record.' + date_obj.strftime('%Y%m%d')
    setid = setid_template.format('HVRes' if restricted else 'HVUnRes')
    provider = 'mckesson_res' if restricted else 'mckesson'
    restriction_level = 'restricted' if restricted else 'unrestricted'
    feed_id = '36' if restricted else '33'
    vendor_id = '119' if restricted else '86'

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


def run(spark_in, runner_in, date_input, mode, test=False, airflow_test=False):
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
        res_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/input-res/'
        ) + '/'
        res_matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/matching-res/'
        ) + '/'
    elif airflow_test:
        unres_input_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_matching_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        res_input_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_res/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        res_matching_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_res/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        unres_input_path = 's3a://salusv/incoming/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )
        res_input_path = 's3a://salusv/incoming/pharmacyclaims/mckesson_res/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_matching_path = 's3a://salusv/matching/payload/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )
        res_matching_path = 's3a://salusv/matching/payload/pharmacyclaims/mckesson_res/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2010-03-01'
    max_date = date_input

    def normalize(input_path, matching_path, restricted=False):
        """
        Generic function for running normalization in any mode
        """
        restriction_level = 'restricted' if restricted else 'unrestricted'

        load(input_path, restriction_level)

        payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

        normalized_output = runner.run_spark_script('normalize.sql', [
            ['min_date', min_date],
            ['max_date', max_date],
            ['restriction_level', restriction_level, False]
        ], return_output=True)

        schema_enforcer.apply_schema(
            normalized_output, pharma_schemas['schema_v6'].schema_structure
        ).createOrReplaceTempView('{}_pharmacyclaims_common_model'.format(restriction_level))

        test_dir = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/output/'
        ) + '/' if test else None

        postprocess_and_unload(date_input, restricted, test_dir)

    # run normalization for appropriate mode(s)
    if mode in ['restricted', 'both']:
        normalize(res_input_path, res_matching_path, True)

    if mode in ['unrestricted', 'both']:
        normalize(unres_input_path, unres_matching_path)

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='McKessonRx{}'.format('-Restricted' if mode == 'restricted' else ''),
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=unres_input_path,
            data_source_matching_path=unres_matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):

    # init spark
    spark, sqlContext = init("McKessonRx")

    # initialize runner
    runner = Runner(sqlContext)

    # figure out what mode we're in
    if args.restricted == args.unrestricted:
        mode = 'both'
    elif args.restricted:
        mode = 'restricted'
    elif args.unrestricted:
        mode = 'unrestricted'

    run(spark, runner, args.date, mode, airflow_test=args.airflow_test)

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
    parser.add_argument('--restricted', default=False, action='store_true')
    parser.add_argument('--unrestricted', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
