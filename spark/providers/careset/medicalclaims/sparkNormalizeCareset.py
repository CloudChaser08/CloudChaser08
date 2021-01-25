from datetime import datetime, date
import argparse

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.medicalclaims_common_model_v2 import schema
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '57'
VENDOR_ID = '244'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/careset/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/custom/careset/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    setid = 'hcpcs_careset_npi_{}.csv'.format(date_input.replace('-', '_'))

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/careset/medicalclaims/resources/input/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/careset/out/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/medicalclaims/careset/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        FEED_ID,
        None,
        'HVM_AVAILABLE_HISTORY_START_DATE'
    )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    from spark.providers.careset.medicalclaims import load_transactions
    load_transactions.load(runner, input_path)

    normalized_df = runner.run_spark_script(
        'normalize.sql',
        [],
        return_output=True
    )

    postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID,
            vendor_id=VENDOR_ID,
            filename=setid,
            model_version_number='02'
        ),
        postprocessor.nullify,
        postprocessor.apply_date_cap(
            runner.sqlContext,
            'date_service',
            max_date,
            FEED_ID,
            None,
            min_date
        ),
        lambda df: medical_priv.filter(df, skip_pos_filter=True)
    )(
        normalized_df
    ).createOrReplaceTempView('medicalclaims_common_model')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            date(1900, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql',
            'careset', 'medicalclaims_common_model',
            'date_service', date_input,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day)
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Careset',
            data_type=DataType.MEDICAL_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path="",
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sql_context = init('Careset')

    runner = Runner(sql_context)

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
