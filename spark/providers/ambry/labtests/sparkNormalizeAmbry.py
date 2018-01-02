#! /usr/bin/python
import argparse
from datetime import datetime

from pyspark.sql.functions import col, explode, split

from spark.runner import Runner
from spark.spark_setup import init

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.labtests as labtests_priv

import logging

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path,  '../../../test/providers/ambry/labtests/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path,  '../../../test/providers/ambry/labtests/resources/payload/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/ambry/out/{}'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/ambry/payload/{}'\
                        .format(date_input.replace('-', '/'))
    else:
        input_path = 's3://salusv/incoming/labtests/ambry/{}/'\
                        .format(date_input.replace('-', '/'))
        input_path = 's3://salusv/matching/payload/labtests/ambry/{}/'\
                        .format(date_input.replace('-', '/'))

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimid'])

    # Create the labtests table to store the results in
    runner.run_spark_script('../../../common/labtests_common_model_v3.sql', [
        ['table_name', 'labtests_common_model', False],
        ['properties', '', False]
    ])
    logging.debug('Created labtests_common_model table')

    # Load the transactions into raw, un-normalized tables
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])
    logging.debug('Loaded the transaction')

    # Explode on Genes_Tested field
    runner.sqlContext.sql('select * from ambry_transactions')   \
          .withColumn('Genes_Tested',                           \
                       explode(split(col('Genes_Tested'), ',')) \
                     )                                          \
          .createTempView('ambry_transactions_gene_exploded')
    logging.debug('Exploded transactions on Gene_Tested field.')

    # Remove leading and trailing whitespace from any strings
    # Nullify rows that require it
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from ambry_transactions_gene_exploded')
    ).createTempView('ambry_transactions')
    logging.debug('Trimmed and nullified data')

    # Create exploder table for pivoting ICD10 codes
    explode.generate_exploder_table(spark, 12)

    # Normalize
    runner.run_spark_script('normalize.sql', [])
    logging.debug('Finished normalizing')

    # Postprocessing
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(
            feed_id='43',
            vendor_id='194',
            filename='plain.txt'.format(date_obj.strftime('%Y%m%d')),   #NOTE: will need to change once known
            model_version_number='04'
        ),
        labtests_priv.filter
    )(
        runner.sqlContext.sql('select * from labtests_common_model')
    ).createTempView('labtests_common_model')
    logging.debug('Finished post-processing')
    
    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'labtests', 'labtests_common_model_v3.sql', 'ambry',
            'labtests_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init('Ambry')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/ambry/labtests/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/text/labtests/ambry/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

