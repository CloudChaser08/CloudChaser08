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

import logging
import load_records

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

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

        load_records.load(runner, input_pth_prefix, product, date_input)
        payload_loader.load(runner, matching_pth, ['claimId'])
        runner.sqlContext.sql('SELECT * FROM matching_payload') \
                .distinct() \
                .createOrReplaceTempView('matching_payload')

        # Normalize
        norm1 = schema_enforcer.apply_schema(
            runner.run_spark_script('mapping_1.sql', return_output=True),
            medicalclaims_schema
        ).distinct()
        norm1.createOrReplaceTempView('medicalclaims_common_model')
        norm2 = schema_enforcer.apply_schema(
            runner.run_spark_script('mapping_2.sql', return_output=True),
            medicalclaims_schema
        ).distinct()
        norm3 = schema_enforcer.apply_schema(
            runner.run_spark_script('mapping_3.sql', return_output=True),
            medicalclaims_schema
        ).distinct()



        product_norm = norm1.union(norm2).union(norm3)
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
        ).repartition(1000)
        logging.debug('Finished post-processing')

        all_norm = product_norm if all_norm is None else all_norm.union(product_norm)

    all_norm.createOrReplaceTempView('medicalclaims_common_model')
    
    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'ability',
            'medicalclaims_common_model', 'date_service', date_input,
            hvm_historical_date = hvm_historical
        )


def main(args):
    # init
    spark, sqlContext = init('Ability')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/medicalclaims/ability/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/medicalclaims/2017-02-24/'

#    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
