#! /usr/bin/python
import argparse
from datetime import datetime, date

from spark.runner import Runner
from spark.spark_setup import init

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
from spark.common.medicalclaims_common_model import schema_v2 as medicalclaims_schema
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.privacy.medicalclaims as medical_priv
from pyspark.sql.utils import AnalysisException

import logging
import load_records

from spark.common.utility import logger, get_spark_time
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility.output_type import DataType, RunType


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/medicalclaims/ability/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/medicalclaims/2017-02-24/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__
    spark.sparkContext.setCheckpointDir('hdfs:///tmp/checkpoints/')

    if test:
        input_path_prefix = file_utils.get_abs_path(
            script_path,  '../../../test/providers/ability/medicalclaims/resources/input/{}'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path,  '../../../test/providers/ability/medicalclaims/resources/payload/{}'
        ) + '/'
    elif airflow_test:
        input_path_prefix = 's3://salusv/testing/dewey/airflow/e2e/ability/medicalclaims/out/{}/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/ability/medicalclaims/payload/{}/'
    else:
        input_path_prefix = 's3://salusv/incoming/medicalclaims/ability/{}/'
        matching_path = 's3://salusv/matching/payload/medicalclaims/ability/{}/'

    external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
                    runner.sqlContext,
                    '15',
                    None,
                    'EARLIEST_VALID_SERVICE_DATE'
                )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    all_norm = None
    for product in ['ap', 'ses', 'ease']:

        set_id = date_input.replace('-', '_') + '_' + product

        input_pth_prefix = input_path_prefix.format(date_input.replace('-', '/')) + set_id + '_'
        matching_pth     = matching_path.format(date_input.replace('-', '/')) + set_id + '_*'

        try:
            load_records.load(runner, input_pth_prefix, product, date_input, test=test)
        except AnalysisException as e:
            if 'Path does not exist' in str(e):
                continue
            else:
                raise(e)

        payload_loader.load(runner, matching_pth, ['claimId'], partitions=1 if test else 500, cache=True)
        runner.sqlContext.sql('SELECT * FROM matching_payload') \
                .distinct() \
                .createOrReplaceTempView('matching_payload')

        # Normalize
        norm1 = schema_enforcer.apply_schema(
            runner.run_spark_script('mapping_1.sql', [['min_date', min_date]], return_output=True),
            medicalclaims_schema
        ).distinct().repartition(1 if test else 500, 'claim_id').checkpoint()
        norm1.createOrReplaceTempView('medicalclaims_common_model')
        remaining_diags = runner.run_spark_script('mapping_pre_2.sql', return_output=True) \
            .distinct().repartition(1 if test else 500, 'claimid')
        remaining_diags.createOrReplaceTempView('remaining_diagnosis')
        norm2 = schema_enforcer.apply_schema(
            runner.run_spark_script('mapping_2.sql', [['min_date', min_date]], return_output=True),
            medicalclaims_schema
        ).distinct().repartition(1 if test else 500, 'claim_id').checkpoint()
        norm3 = schema_enforcer.apply_schema(
            runner.run_spark_script('mapping_3.sql', [['min_date', min_date]], return_output=True),
            medicalclaims_schema
        ).distinct()



        product_norm = norm1.union(norm2).union(norm3).checkpoint()
        logging.debug('Finished normalizing')

        hvm_historical = postprocessor.coalesce_dates(
                        runner.sqlContext,
                        '15',
                        date(1901, 1, 1),
                        'HVM_AVAILABLE_HISTORY_START_DATE',
                        'EARLIEST_VALID_SERVICE_DATE'
        )
        hvm_historical = datetime(hvm_historical.year, hvm_historical.month, hvm_historical.day)

        product_norm = postprocessor.compose(
            schema_enforcer.apply_schema_func(medicalclaims_schema),
            postprocessor.nullify,
            medical_priv.filter,
            postprocessor.add_universal_columns(
                feed_id='15',
                vendor_id='14',
                filename=set_id,
                model_version_number='02'
            ),
            *[postprocessor.apply_date_cap(runner.sqlContext, c, max_date, '15', None, min_date)
                for c in ['date_received', 'date_service', 'date_service_end']]
        )(
            product_norm
        ).repartition(1000, 'claim_id')
        logging.debug('Finished post-processing')

        all_norm = product_norm if all_norm is None else all_norm.union(product_norm)

    all_norm.createOrReplaceTempView('medicalclaims_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'ability',
            'medicalclaims_common_model', 'date_service', date_input,
            hvm_historical_date = hvm_historical
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Ability',
            data_type=DataType.MEDICAL_CLAIMS,
            data_source_transaction_path=input_path_prefix,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )

def main(args):
    # init
    spark, sqlContext = init('Ability')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark_runtime = get_spark_time()

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(spark_app_runtime=spark_runtime, additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
