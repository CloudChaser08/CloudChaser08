from datetime import datetime, date
import argparse

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.medicalclaims_common_model import schema_v6 as schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as med_priv
import spark.providers.allscripts.medicalclaims.transactions as transactions
import spark.providers.allscripts.medicalclaims.udf as allscripts_udf
import spark.helpers.explode as explode

from pyspark.sql.types import ArrayType, StringType

FEED_ID = '26'
VENDOR_ID = '35'

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/medicalclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/medicalclaims/resources/matching/'
        ) + '/'
        output_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/medicalclaims/output/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        output_path = ''
    else:
        input_path = 's3a://salusv/incoming/medicalclaims/allscripts/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/medicalclaims/allscripts/{}/'.format(
            date_input.replace('-', '/')
        )
        output_path = ''

    runner.sqlContext.registerFunction(
        'linked_and_unlinked_diagnoses', allscripts_udf.linked_and_unlinked_diagnoses, ArrayType(ArrayType(StringType()))
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
    payload_loader.load(runner, matching_path, ['PCN', 'hvJoinKey'])
    records_loader.load_and_clean_all(runner, input_path, transactions, 'csv', '|', partitions=1000)
    explode.generate_exploder_table(spark, 8, 'diag_exploder')

    runner.run_spark_script('pre_normalization.sql', return_output=True) \
            .createOrReplaceTempView('tmp')
    normalized = runner.run_spark_script('normalize.sql', return_output=True)

    postprocessed = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID,
            vendor_id=VENDOR_ID,
            filename='HV_CLAIMS_' + date_input.replace('-', ''),
            model_version_number='06'
        ),
        postprocessor.nullify,
        med_priv.filter,
        postprocessor.apply_date_cap(
            runner.sqlContext,
            'date_service',
            max_date,
            FEED_ID,
            None,
            min_date
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext,
            'date_service_end',
            max_date,
            FEED_ID,
            None,
            min_date
        ),
        schema_enforcer.apply_schema_func(schema)
    )(
        normalized
    )

    hvm_historical = postprocessor.coalesce_dates(
        runner.sqlContext,
        FEED_ID,
        date(1900, 1, 1),
        'HVM_AVAILABLE_HISTORY_START_DATE',
        'EARLIST_VALID_SERVICE_DATE'
    )

    normalized_records_unloader.unload(
        spark, runner, postprocessed, 'date_service', date_input, 'allscripts',
        hvm_historical_date=datetime(hvm_historical.year,
                                        hvm_historical.month,
                                        hvm_historical.day),
        test_dir=(output_path if test else None)
    )


def main(args):
    spark, sqlContext = init('Allscripts')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/medicalclaims/2018-06-06/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)