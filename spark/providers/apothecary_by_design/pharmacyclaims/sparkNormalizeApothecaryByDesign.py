from datetime import datetime
from pyspark.sql.functions import isnull, lead
from pyspark.sql import Window
import argparse

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

def run(spark, runner, date_input, test = False, airflow_test = False):
    setid = 'hv_export_data_{}'.format(date_input.replace('-', ''))

    script_path = __file__

    if test:
        txn_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/apothecary_by_design/pharmacyclaims/resources/txn_input/' 
        )
        add_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/apothecary_by_design/pharmacyclaims/resources/add_input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/apothecary_by_design/pharmacyclaims/resources/matching/'
        )
    elif airflow_test:
        txn_input_path = 's3://salusv/testing/dewey/airflow/e2e/abd/pharmacyclaims/out/transaction/{}/'.format(
            date_input.replace('-', '/')
        )
        add_input_path = 's3://salusv/testing/dewey/airflow/e2e/abd/pharmacyclaims/out/additional/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/abd/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        txn_input_path = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/{}/transactions/'.format(
            date_input.replace('-', '/')
        )
        add_input_path = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/{}/additionaldata/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/apothecarybydesign/{}/'.format(
            date_input.replace('-', '/')
        )

    external_table_loader.load_ref_gen_ref(runner.sqlContext)

    if test:
        min_date = '1900-01-01'
    else:
        min_date = postprocessor.get_gen_ref_date(
            runner.sqlContext,
            '45',
            'EARLIEST_VALID_SERVICE_DATE'
        ).isoformat()
    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v3.sql', [
        ['external', '', False],
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False]
    ])

    # Load in the matching payload
    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'hvJoinKey'])

    # Point hive to the location of the transaction data
    # and describe its schema
    runner.run_spark_script('load_transactions.sql', [
        ['txn_input_path', txn_input_path],
        ['add_input_path', add_input_path]
    ])

    # Remove leading and trailing whitespace from any strings
    # in the two tables
    postprocessor.trimmify(
        runner.sqlContext.sql('select * from abd_transactions')
    ).createTempView('abd_transactions_with_dupes')

    postprocessor.trimmify(
        runner.sqlContext.sql('select * from abd_additional_data')
    ).createTempView('abd_additional_data_with_dupes')

    # De-dupe both of the tables
    # - Transaction: Only difference is hvJoinKey, can just
    #                use dropDuplicates
    # - Additional: Difference is the ticket_dt, we want to keep
    #               the row with the most recent ticket_dt
    runner.sqlContext.sql('select * from abd_transactions_with_dupes') \
            .dropDuplicates(['sales_id']) \
            .createTempView('abd_transactions')
    window = Window.orderBy('ticket_dt').partitionBy('sales_cd')
    runner.sqlContext.sql('select * from abd_additional_data_with_dupes') \
            .withColumn('next_ticket_dt', lead('ticket_dt', 1).over(window)) \
            .where(isnull('next_ticket_dt')) \
            .drop('next_ticket_dt') \
            .createTempView('abd_additional_data')

    # Run the normalization script on the transaction data
    # and matching payload
    runner.run_spark_script('normalize.sql', [])

    # Apply clean up and privacy filtering
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id = '45', vendor_id = '204', filename = setid),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service', max_date, '45', 'EARLIEST_VALID_SERVICE_DATE'),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        hvm_historical = postprocessor.get_gen_ref_date(
            runner.sqlContext,
            '45',
            'HVM_AVAILABLE_HISTORY_START_DATE'
        )
        if hvm_historical is None:
            hvm_historical = postprocessor.get_gen_ref_date(
                runner.sqlContext,
                '45',
                'EARLIEST_VALID_SERVICE_DATE'
            )
        if hvm_historical is None:
            hvm_historical = date(1901, 1, 1) 

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v3.sql',
            'apothecary_by_design', 'pharmacyclaims_common_model',
            'date_service', date_input,
            hvm_historical_date = datetime(hvm_historical.year,
                                           hvm_historical.month,
                                           hvm_historical.day)
        )


def main(args):
    spark, sqlContext = init('Apothecary_By_Design')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/apothecarybydesign/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)


