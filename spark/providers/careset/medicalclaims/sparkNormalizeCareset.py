from datetime import datetime, date
import argparse

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.medicalclaims_common_model_v2 import schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv
import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

FEED_ID = '57'
VENDOR_ID = '244'

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

    from . import load_transactions
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


def main(args):
    spark, sqlContext = init('Careset')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/careset/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/custom/careset/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
