import argparse
from datetime import datetime, date

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.lab_common_model import schema_v7 as schema

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.labtests as lab_priv

from pyspark.sql.functions import col, explode

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '76'
VENDOR_ID = '314'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/genomind/labtests/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/labtests/2018-02-09/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__
    setid = 'PatientData.out'  # TODO: fill in correctly when known
    max_cap = date_input
    max_cap_obj = datetime.strptime(max_cap, '%Y-%m-%d')

    input_tables = [
        'clinicians',
        'diagnosis',
        'genes',
        'medications',
        'patient'
    ]

    input_paths = {}
    if test:
        for t in input_tables:
            input_paths[t] = file_utils.get_abs_path(
                script_path, '../../../test/providers/genomind/labtests/resources/input/{}'.format(t)
            )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/genomind/labtests/resources/payload/'
        )
    elif airflow_test:
        for t in input_tables:
            input_paths[t] = 's3://salusv/testing/dewey/airflow/e2e/genomind/input/{}'.format(t)
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/genomind/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        for t in input_tables:
            input_paths[t] = 's3://salusv/incoming/labtests/genomind/{}/{}/'.format(
                date_input.replace('-', '/'), t
            )
        matching_path = 's3://salusv/matching/payload/labtests/genomind/{}/'.format(
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

    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'patientId', 'hvJoinKey'])

    import spark.providers.genomind.labtests.load_transactions as load_transactions
    load_transactions.load(runner, input_paths)

    # De-duplicate the source tables
    runner.sqlContext.sql('select * from diagnosis') \
                     .dropDuplicates(['patientkey', 'icdcodetype', 'icdcode']) \
                     .createOrReplaceTempView('diagnosis_dedup')
    runner.sqlContext.sql('select * from genes') \
                     .dropDuplicates(['patientkey', 'genename', 'pkphenotype']) \
                     .createOrReplaceTempView('genes_dedup')
    runner.sqlContext.sql('select * from medications') \
                     .dropDuplicates(['patientkey', 'genericname', 'dosage']) \
                     .createOrReplaceTempView('medications_dedup')

    # Explode the clinicians table
    runner.sqlContext.sql('select * from clinicians') \
                     .withColumn('PatientKey', explode(col('Patients List'))) \
                     .drop('Patients List') \
                     .createOrReplaceTempView('clinicians_explode')

    # Run the normalization
    normalized_df = runner.run_spark_script(
        'normalize.sql',
        [],
        return_output=True
    )

    final_df = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID,
            vendor_id=VENDOR_ID,
            filename=setid,
            model_version_number='07'
        ),
        postprocessor.nullify,
        lab_priv.filter,
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
            'EARLIST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.unload(
            spark, runner, final_df, 'date_service', max_cap,
            'genomind', hvm_historical_date=datetime(hvm_historical.year,
                                                     hvm_historical.month,
                                                     hvm_historical.day)
        )
    else:
        final_df.createOrReplaceTempView('labtests_common_model')

    if not test and not airflow_test:
        # TODO: Determine Transaction path
        logger.log_run_details(
            provider_name='Geomind',
            data_type=DataType.LAB_TESTS,
            data_source_transaction_path="",
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sqlContext = init('Genomind Normalization')

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
