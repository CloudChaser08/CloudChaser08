import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


TODAY = time.strftime('%Y-%m-%d', time.localtime())

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/diplomat/pharmacyclaims/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'HealthVerityOut_' + date_obj.strftime('%Y%m%d') + '.csv'

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/diplomat/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/diplomat/pharmacyclaims/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/diplomat/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/diplomat/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/pharmacyclaims/diplomat/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/pharmacyclaims/diplomat/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2014-01-01'
    min_date_written = '2004-01-01'
    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims/sql/pharmacyclaims_common_model_v3.sql', [
        ['table_name', 'pharmacyclaims_common_model', False],
        ['external', '', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    serde_props = "WITH SERDEPROPERTIES ('separatorChar' = '|')" \
        if date_input > '2017-07-31' else ''

    if date_input > '2017-06-05':
        runner.run_spark_script('load_transactions_v2.sql', [
            ['input_path', input_path],
            ['serde_props', serde_props, False]
        ])
    else:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path],
            ['serde_props', serde_props, False]
        ])
    postprocessor.trimmify(runner.sqlContext.sql('select * from transactions')).createTempView('transactions')

    runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['min_date_written', min_date_written],
        ['max_date', max_date]
    ])

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='34', vendor_id='100', filename=setid),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims/sql/pharmacyclaims_common_model_v3.sql', 'diplomat',
            'pharmacyclaims_common_model', 'date_service', date_input
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='DiplomatRX',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sqlContext = init("DiplomatRX")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
