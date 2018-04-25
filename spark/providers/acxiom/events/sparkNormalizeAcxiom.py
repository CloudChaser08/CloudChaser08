import argparse
from datetime import datetime, date
from pyspark.sql.functions import lit

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.event_common_model_v6 import schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.events as event_priv

FEED_ID = '50'
VENDOR_ID = '229'

def run(spark, runner, date_input, test=False, airflow_test=False):
    setid = 'acxiom'

    script_path = __file__

    if test:
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/acxiom/events/resources/matching/'
        )
        ids_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/acxiom/events/resources/ids/'
        )
    elif airflow_test:
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/acxiom/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        ids_path = 's3://salusv/testing/dewey/airflow/e2e/acxiom/ids/'
    else:
        matching_path = 's3://salusv/matching/payload/consumer/acxiom/{}/'.format(
            date_input.replace('-', '/')
        )
        ids_path = 's3://salusv/reference/acxiom/ids/'

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

    payload_loader.load(runner, matching_path, ['claimId'])

    normalized_df = runner.run_spark_script(
        'normalize.sql',
        [['date_input', date_input]],
        return_output=True
    )

    events_df = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID,
            vendor_id=VENDOR_ID,
            filename=setid,
            model_version_number='06'
        ),
        postprocessor.nullify,
        event_priv.filter
    )(
        normalized_df
    )

    # Remove any source ids that are not in the acxiom ids table
    if test:
        from pyspark.sql.types import *
        acxiom_ids = runner.sqlContext.read.csv(ids_path, StructType([StructField('aid', StringType(), True)]), sep='|')
    else:
        acxiom_ids = runner.sqlContext.read.parquet(ids_path)

    valid_aid = events_df.join(acxiom_ids, events_df.source_record_id == acxiom_ids.aid, 'leftsemi')
    non_valid_aid = events_df.join(acxiom_ids, events_df.source_record_id == acxiom_ids.aid, 'leftanti') \
                                 .withColumn('logical_delete_reason', lit('DELETE'))

    validated_data = valid_aid.union(non_valid_aid).createOrReplaceTempView('event_common_model')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            date(1900, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'events', 'event_common_model_v4.sql',
            'acxiom', 'event_common_model',
            'source_record_date', date_input,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day)
        )


def main(args):
    spark, sqlContext = init('Acxiom')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/acxiom/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/consumer/2017-08-02/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

