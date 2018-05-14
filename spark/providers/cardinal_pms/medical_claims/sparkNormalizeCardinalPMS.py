#! /usr/bin/python
import argparse
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql.functions import col, collect_set, explode, first

from spark.runner import Runner
from spark.spark_setup import init

from spark.common.medicalclaims_common_model_v5 import schema

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as exploder
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv

import logging

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path,  '../../../test/providers/cardinal_pms/medicalclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path,  '../../../test/providers/cardinal_pms/medicalclaims/resources/payload/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/out/{}'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/payload/{}'\
                        .format(date_input.replace('-', '/'))
    else:
        input_path = 's3://salusv/incoming/medicalclaims/cardinal_pms/{}/'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/matching/payload/medicalclaims/cardinal_pms/{}/'\
                        .format(date_input.replace('-', '/'))

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    # Load the transactions into raw, un-normalized tables
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])
    logging.debug('Loaded the transaction')

    # Remove leading and trailing whitespace from any strings
    # Nullify rows that require it
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from transactional_cardinal_pms')
    ).createTempView('transactional_cardinal_pms')
    logging.debug('Trimmed and nullified data')

    # Create exploder table for service-line
    exploder.generate_exploder_table(spark, 5, 'service_line_exploder')
    logging.debug('Created exploder table for service-line')

    # Normalize service-line
    service_lines = runner.run_spark_script('normalize_service_line.sql', [], return_output=True)
    logging.debug('Finished normalizing for service-line')

    # Create a table that contains one row for each claim
    # where the service line is the lowest number
    window = Window.partitionBy(col('ediclaim_id')).orderBy(col('linesequencenumber').desc())
    runner.sqlContext.sql('select * from transactional_cardinal_pms')           \
          .withColumn('first', first(col('linesequencenumber')).over(window))   \
          .where(col('linesequencenumber') == col('first'))                     \
          .drop(col('first'))                                                   \
          .createTempView('limited_transactional_cardinal_pms')

    # Create a table that contains a each unique
    # diagnosis code and claim id
    service_lines                                                               \
          .groupby(col('claim_id'))                                             \
          .agg(collect_set(col('diagnosis_code')).alias('diagnosis_codes'))     \
          .withColumn('diagnosis_code', explode(col('diagnosis_codes')))        \
          .select(col('claim_id'), col('diagnosis_code'))                       \
          .createTempView('service_line_diags')

    # Create exploder table for claim
    exploder.generate_exploder_table(spark, 8, 'claim_exploder')
    logging.debug('Created exploder for claim')

    # Normalize claim
    claim_lines = runner.run_spark_script('normalize_claim.sql', [], return_output=True)
    logging.debug('Finished normalizing for claim')

    # Postprocessing
    service_lines_final = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.nullify,
        postprocessor.add_universal_columns(
            feed_id='41',
            vendor_id='188',
            filename='PMS_record_data_{}'.format(date_obj.strftime('%Y%m%d'))
        ),
        medical_priv.filter,
        schema_enforcer.apply_schema_func(schema)
    )(
       service_lines
    )

    claim_lines_final = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.nullify,
        postprocessor.add_universal_columns(
            feed_id='41',
            vendor_id='188',
            filename='PMS_record_data_{}'.format(date_obj.strftime('%Y%m%d'))
        ),
        medical_priv.filter,
        schema_enforcer.apply_schema_func(schema)
    )(
        claim_lines
    )

    pms_data_final = service_lines_final.union(claim_lines_final)
    logging.debug('Finished post-processing')
    
    if not test:
        normalized_records_unloader.unload(
            spark, runner, pms_data_final, 'date_service', date_input, 'cardinal_pms'
        )
    else:
        pms_data_final.createOrReplaceTempView('medicalclaims_common_model')


def main(args):
    # init
    spark, sqlContext = init('Cardinal PMS')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/medical_claims/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/text/medical_claims/cardinal_pms/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
