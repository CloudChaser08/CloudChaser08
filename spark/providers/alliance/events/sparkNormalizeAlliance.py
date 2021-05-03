import argparse
from datetime import date, datetime
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.event_common_model_v7 import schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.events as events_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '56'
LATEST_ACTIVES_DATE = '2018-09-25'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/alliance/spark-output/'
OUTPUT_PATH_PRODUCTION = ""


def run(spark, runner, date_input, project_id, test=False, airflow_test=False):
    script_path = __file__

    normalize_full_set = False if project_id else True

    setid = 'HVRequest_{}_return.csv'.format(project_id) if project_id \
            else 'healthverity_sample_1MM_20180309.txt'

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/alliance/events/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/alliance/events/resources/payload/'
        )
        actives_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/alliance/events/resources/actives/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/alliance/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/alliance/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        actives_path = 's3://salusv/testing/dewey/airflow/e2e/alliance/actives/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/consumer/alliance/projects/{}/{}/'.format(
            project_id, date_input.replace('-', '/')
        ) if project_id else 's3://salusv/incoming/consumer/alliance/2018/03/12/onemillion/'
        matching_path = 's3://salusv/matching/payload/consumer/alliance/*/*/*'
        #  NOTE: alliance sends a full refresh of actives each time (mostly on an ad-hoc basis)
        #      You will need to swap the warehouse data each time this is ran
        actives_path = 's3://salusv/incoming/consumer/alliance/actives/{}/'.format(
            LATEST_ACTIVES_DATE.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        FEED_ID,
        None,
        'EARLIEST_VALID_SERVICE_DATE'
    )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    payload_loader.load(
        runner, matching_path, ['claimId', 'personId', 'hvJoinKey'], partitions=10 if test else 1000
    )

    spark.table('matching_payload') \
         .select('hvid', 'age', 'yearOfBirth', 'threeDigitZip', 'state', 'gender', 'personid') \
         .distinct() \
         .createOrReplaceTempView('matching_payload')

    import spark.providers.alliance.events.load_transactions as load_transactions
    load_transactions.load(runner, input_path, actives_path)

    # Normalize the source data
    normalized_df = runner.run_spark_script(
        'normalize.sql', [
            ['join_type', 'RIGHT' if normalize_full_set else 'INNER', False]
        ], return_output=True
    )

    # Post-processing on the normalized data
    alliance_data_final = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID,
            vendor_id='243',
            filename=setid,
            model_version_number='07'
        ),
        postprocessor.nullify,
        postprocessor.apply_date_cap(
            runner.sqlContext,
            'event_date',
            max_date,
            FEED_ID,
            None,
            min_date
        ),
        postprocessor.apply_whitelist(
            runner.sqlContext,
            'event_category_code',
            'transaction.naics_code',
            [
                'event_category_code_qual',
                'event_category_name'
            ],
            whitelist_col_name='gen_ref_cd',
            feed_id=FEED_ID
        ),
        events_priv.filter,
        schema_enforcer.apply_schema_func(schema)
    )(
        normalized_df
    )

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            date(1900, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.unload(
            spark, runner, alliance_data_final, 'event_date', max_date, 'alliance',
            hvm_historical_date=datetime(
                hvm_historical.year,
                hvm_historical.month,
                hvm_historical.day
            )
        )

    else:
        alliance_data_final.createOrReplaceTempView('event_common_model')

    if not test and not airflow_test:
        # TODO: Determine DataType
        logger.log_run_details(
            provider_name='Alliance',
            data_type=DataType.CONSUMER,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sql_context = init('Alliance Normalization')

    runner = Runner(sql_context)
    output_path = None
    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    else:
        OUTPUT_PATH_PRODUCTION = args.output_path
        ouput_path = OUTPUT_PATH_PRODUCTION

    run(spark, runner, args.date, args.project_id, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--project_id', type=str, default=None)
    args = parser.parse_known_args()[0]
    main(args)
