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

FEED_ID = '56'

def run(spark, runner, date_input, project_id, test=False, airflow_test=False):
    script_path = __file__

    setid = 'HVRequest_{}_return.csv'.format(project_id)

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/alliance/events/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/alliance/events/resources/payload/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/alliance/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/alliance/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/consumer/alliance/projects/{}/{}/'.format(
            project_id, date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/consumer/alliance/*/*/*'

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

    import spark.providers.alliance.events.load_transactions as load_transactions
    load_transactions.load(runner, input_path)

    # Normalize the source data
    normalized_df = runner.run_spark_script(
        'normalize.sql',
        [],
        return_output=True
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


def main(args):
    spark, sqlContext = init('Alliance Normalization')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, args.project_id, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/alliance/spark-output/'
    else:
        output_path = args.output_path

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--project_id', type=str)
    args = parser.parse_args()
    main(args)