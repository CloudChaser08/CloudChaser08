"""
cardinal medicalclaims census schema
"""
#! /usr/bin/python
import argparse
from datetime import datetime

from pyspark.sql import Window
import pyspark.sql.functions as FN

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.medicalclaims import schemas

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as exploder
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv

import logging

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility import logger

schema = schemas["schema_v8"].schema_structure

# staging for deliverable
DELIVERABLE_LOC = 'hdfs:///deliverable/'

DEOBFUSCATION_KEY = 'Cardinal_MPI-0'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/medicalclaims/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/medicalclaims/2017-02-24/'


def run(spark, runner, date_input, batch_id, test=False, airflow_test=False):
    global DELIVERABLE_LOC
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/cardinal_pms/medicalclaims_census/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/cardinal_pms/medicalclaims_census/resources/payload/'
        ) + '/'
        DELIVERABLE_LOC = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/cardinal_pms/medicalclaims_census/resources/delivery/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/out/{}/'\
                        .format(batch_id if batch_id else date_input.replace('-', '/'))
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/payload/{}/'\
                        .format(batch_id if batch_id else date_input.replace('-', '/'))
    else:
        input_path = 's3://salusv/incoming/medicalclaims/cardinal_pms/{}/'\
                        .format(batch_id if batch_id else date_input.replace('-', '/'))
        matching_path = 's3://salusv/matching/payload/medicalclaims/cardinal_pms/{}/'\
                        .format(batch_id if batch_id else date_input.replace('-', '/'))

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    # Load the transactions into raw, un-normalized tables
    input_column_length = len(spark.read.csv(input_path, sep='|').columns)
    if input_column_length == 90:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path]
        ])
        runner.sqlContext.sql('select * from transactional_cardinal_pms_temp') \
                         .withColumn('product_service_id_qualifier', FN.lit(None)) \
                         .withColumn('product_service_id', FN.lit(None)) \
                         .withColumn('diagnosisnine', FN.lit(None)) \
                         .withColumn('diagnosisten', FN.lit(None)) \
                         .withColumn('diagnosiseleven', FN.lit(None)) \
                         .withColumn('diagnosistwelve', FN.lit(None)) \
                         .withColumnRenamed('claim_lines_id', 'id_3') \
                         .createOrReplaceTempView('transactional_cardinal_pms')
    elif input_column_length == 92:
        runner.run_spark_script('load_transactions_v2.sql', [
            ['input_path', input_path]
        ])
        runner.sqlContext.sql('select * from transactional_cardinal_pms_temp') \
                         .withColumn('diagnosisnine', FN.lit(None)) \
                         .withColumn('diagnosisten', FN.lit(None)) \
                         .withColumn('diagnosiseleven', FN.lit(None)) \
                         .withColumn('diagnosistwelve', FN.lit(None)) \
                         .withColumnRenamed('claim_lines_id', 'id_3') \
                         .createOrReplaceTempView('transactional_cardinal_pms')
    elif input_column_length == 96:
        runner.run_spark_script('load_transactions_v3.sql', [
            ['input_path', input_path]
        ])
    else:
        runner.run_spark_script('load_transactions_v4.sql', [
            ['input_path', input_path]
        ])

    logging.debug('Loaded the transaction')

    # Remove leading and trailing whitespace from any strings
    # Nullify rows that require it
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from transactional_cardinal_pms')
    ).createOrReplaceTempView('transactional_cardinal_pms')
    logging.debug('Trimmed and nullified data')

    # Create exploder table for service-line
    exploder.generate_exploder_table(spark, 5, 'service_line_exploder')
    logging.debug('Created exploder table for service-line')

    # Normalize service-line
    service_lines = runner.run_spark_script(
        'normalize_service_line.sql', [], return_output=True, source_file_path=script_path
    )
    logging.debug('Finished normalizing for service-line')

    # Create a table that contains one row for each claim
    # where the service line is the lowest number
    window = Window.partitionBy(FN.col('ediclaim_id')).orderBy(FN.col('linesequencenumber').desc())
    runner.sqlContext.sql('select * from transactional_cardinal_pms')           \
          .withColumn('first', FN.first(FN.col('linesequencenumber')).over(window))   \
          .where(FN.col('linesequencenumber') == FN.col('first'))                     \
          .drop(FN.col('first'))                                                   \
          .createOrReplaceTempView('limited_transactional_cardinal_pms')

    # Create a table that contains a each unique
    # diagnosis code and claim id
    service_lines                                                               \
        .groupby(FN.col('claim_id'))                                             \
        .agg(FN.collect_set(FN.col('diagnosis_code')).alias('diagnosis_codes'))     \
        .withColumn('diagnosis_code', FN.explode(FN.col('diagnosis_codes')))        \
        .select(FN.col('claim_id'), FN.col('diagnosis_code'))                       \
        .createOrReplaceTempView('service_line_diags')

    # Create exploder table for claim
    exploder.generate_exploder_table(spark, 12, 'claim_exploder')
    logging.debug('Created exploder for claim')

    # Normalize claim
    claim_lines = runner.run_spark_script(
        'normalize_claim.sql', [], return_output=True, source_file_path=script_path
    )
    logging.debug('Finished normalizing for claim')

    service_lines_schema = schema_enforcer.apply_schema(service_lines, schema)
    claim_lines_schema = schema_enforcer.apply_schema(claim_lines, schema)

    claim_window = Window.partitionBy(FN.col('claim_id'))
    pms_data = service_lines_schema.union(claim_lines_schema)

    # modify date_service and date_service_end columns to use the
    # min(date_service) or max(date_service), respectively, over the
    # entire claim for claim-level rows
    pms_data = pms_data.withColumn(
        'date_service_end', FN.when(
            FN.col('service_line_number').isNull(), FN.max('date_service').over(claim_window)
        ).otherwise(FN.col('date_service_end'))
    ).withColumn(
        'date_service', FN.when(
            FN.col('service_line_number').isNull(), FN.min('date_service').over(claim_window)
        ).otherwise(FN.col('date_service'))
    )

    # Postprocessing
    final_df = postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(
            feed_id='41',
            vendor_id='188',
            filename='pms_record.{}'.format(date_obj.strftime('%Y%m%d'))
        ),
        medical_priv.filter,
        schema_enforcer.apply_schema_func(schema)
    )(
        pms_data
    )
    final_df.cache_and_track('final')
    final_df.createOrReplaceTempView('medicalclaims')
    logging.debug('Finished post-processing')

    # unload delivery file for cardinal
    normalized_records_unloader.unload_delimited_file(
        spark, runner, DELIVERABLE_LOC, 'medicalclaims', test=test
    )

    #  deobfuscate hvid
    final_df = postprocessor.deobfuscate_hvid(DEOBFUSCATION_KEY, nullify_non_integers=True)(
        final_df
    )

    if not test:
        normalized_records_unloader.unload(
            spark, runner, final_df, 'date_service', date_input, 'cardinal_pms'
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_PMS',
            data_type=DataType.MEDICAL_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.CENSUS,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init('Cardinal PMS')

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date, args.batch_id, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
        deliverable_path = \
            's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/medicalclaims/delivery/{}/'\
            .format(args.batch_id if args.batch_id else args.date.replace('-', '/'))
    else:
        output_path = OUTPUT_PATH_PRODUCTION
        deliverable_path = 's3://salusv/deliverable/cardinal_pms-0/{}/'.format(
            args.batch_id if args.batch_id else args.date.replace('-', '/')
        )

    normalized_records_unloader.distcp(output_path)
    normalized_records_unloader.distcp(deliverable_path, DELIVERABLE_LOC)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--batch_id', type=str, default=None)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
