#! /usr/bin/python
import argparse
from datetime import datetime, date

from pyspark.sql.functions import col, explode, split

from spark.runner import Runner
from spark.spark_setup import init

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.explode as exploder
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.labtests as labtests_priv

import logging

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/ambry/labtests/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/ambry/labtests/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/ambry/labtests/resources/payload/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/ambry/out/{}'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/ambry/payload/{}'\
                        .format(date_input.replace('-', '/'))
    else:
        input_path = 's3://salusv/incoming/labtests/ambry/{}/'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/matching/payload/labtests/ambry/{}/'\
                        .format(date_input.replace('-', '/'))

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        '43',
        None,
        'EARLIEST_VALID_SERVICE_DATE'
    )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimid'])

    # Create the labtests table to store the results in
    runner.run_spark_script('../../../common/lab_common_model_v4.sql', [
        ['table_name', 'labtests_common_model', False],
        ['properties', '', False]
    ])
    logging.debug('Created labtests_common_model table')

    # Load the transactions into raw, un-normalized tables
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])
    logging.debug('Loaded the transaction')

    # Explode on genes_tested field
    runner.sqlContext.sql('select * from ambry_transactions')   \
          .withColumn('genes_tested', explode(split(col('genes_tested'), ',')))\
        .createTempView('ambry_transactions_gene_exploded')
    logging.debug('Exploded transactions on gene_tested field.')

    # Remove leading and trailing whitespace from any strings
    # Nullify rows that require it
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from ambry_transactions_gene_exploded')
    ).createTempView('ambry_transactions')
    logging.debug('Trimmed and nullified data')

    # Create exploder table for pivoting ICD10 codes
    exploder.generate_exploder_table(spark, 12)

    # Normalize
    runner.run_spark_script('normalize.sql')
    logging.debug('Finished normalizing')

    # Postprocessing
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(
            feed_id='43',
            vendor_id='194',
            filename='plain.txt',   # NOTE: will need to change once known
            model_version_number='04'
        ),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service', max_date, '43', None, min_date),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_report', max_date, '43', None, min_date),
        labtests_priv.filter
    )(
        runner.sqlContext.sql('select * from labtests_common_model')
    ).createTempView('labtests_common_model')
    logging.debug('Finished post-processing')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
                        runner.sqlContext,
                        '43',
                        date(1901, 1, 1),
                        'HVM_AVAILABLE_HISTORY_START_DATE',
                        'EARLIEST_VALID_SERVICE_DATE'
        )
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'labtests', 'lab_common_model_v4.sql', 'ambry',
            'labtests_common_model', 'date_service', date_input,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day)
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='AmbryExome',
            data_type=DataType.LAB_TESTS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sqlContext = init('AmbryExome')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

