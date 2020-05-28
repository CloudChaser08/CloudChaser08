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
import spark.providers.allscripts.medicalclaims.transactions_v1 as transactions_v1
import spark.providers.allscripts.medicalclaims.transactions_v2 as transactions_v2
import spark.providers.allscripts.medicalclaims.udf as allscripts_udf
import spark.helpers.explode as explode

from pyspark.sql.types import ArrayType, StringType

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '26'
VENDOR_ID = '35'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/allscripts/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/medicalclaims/2018-06-06/'

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

    # New layout after 2018-07-25, but we already got it once on 2018-07-24
    if date_input > '2018-07-25' or date_input == '2018-07-24':
        records_loader.load_and_clean_all(runner, input_path, transactions_v2, 'csv', '|', partitions=5 if test else 1000)
    else:
        records_loader.load_and_clean_all(runner, input_path, transactions_v1, 'csv', '|', partitions=5 if test else 1000)

    explode.generate_exploder_table(spark, 8, 'diag_exploder')

    runner.run_spark_script('pre_normalization_1.sql', return_output=True) \
            .createOrReplaceTempView('tmp')
    normalized_related = runner.run_spark_script('normalize_1.sql', return_output=True).cache()
    normalized_related.createOrReplaceTempView('related_diagnoses_records')
    normalized_related.count()
    runner.run_spark_script('pre_normalization_2.sql', return_output=True) \
            .createOrReplaceTempView('tmp')
    normalized_unrelated = runner.run_spark_script('normalize_2.sql', return_output=True)

    normalized = schema_enforcer.apply_schema(normalized_related, schema) \
        .union(schema_enforcer.apply_schema(normalized_unrelated, schema))

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

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='AllScripts',
            data_type=DataType.MEDICAL_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):

    # Placeholder to Override Spark Conf. properties (after spark launch)
    spark_conf_parameters = {
        'spark.default.parallelism': 5000,
        'spark.sql.shuffle.partitions': 5000
    }

    spark, sqlContext = init('Allscripts', False, spark_conf_parameters)

    runner = Runner(sqlContext)

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
