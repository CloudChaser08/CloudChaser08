from datetime import datetime, date
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

from . import normalize
from . import load_transactions

def run(spark, runner, date_input, test = False, airflow_test = False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/genoa/pharmacyclaims/resources/input/{}/'.format(
                date_input.replace('-', '/')
            )
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/genoa/pharmacyclaims/resources/matching/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/genoa/out/{}/transactions/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/genoa/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/genoa/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/genoa/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    max_date = date_input

    runner.run_spark_script('../../../common/zip3_to_state.sql')

    # Load in the matching payload
    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    # Genoa sent historical data in one schema, and then added 3 new columns
    # later on
    load_transactions.load(spark, runner.sqlContext, input_path, date_input >= '2017-11-01')

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(
        runner.sqlContext.sql('select * from genoa_rx_raw')
    ).createOrReplaceTempView('genoa_rx_raw')

    # On 2017-11-01, Genoa gave us a restatement of all the data from
    # 2015-05-31 on. If any batches before 2017-11-01 contain data from the
    # restated time period, remove it
    if date_input < '2017-11-01' and date_input >= '2015-05-31':
        runner.sqlContext.sql("select * from genoa_rx_raw where date_of_service < '2015-05-31'") \
            .createOrReplaceTempView('genoa_rx_raw')

    normalize.run(runner)

    # Apply clean up and privacy filtering
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id = '21', vendor_id = '20', filename = None, model_version_number = '06'),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service', max_date, '21', 'EARLIEST_VALID_SERVICE_DATE'),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createOrReplaceTempView('pharmacyclaims_common_model')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            '21',
            date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v6.sql',
            'genoa', 'pharmacyclaims_common_model',
            'date_service', date_input,
            hvm_historical_date = datetime(hvm_historical.year,
                                           hvm_historical.month,
                                           hvm_historical.day)
        )


def main(args):
    spark, sqlContext = init('Genoa')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/genoa/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)


