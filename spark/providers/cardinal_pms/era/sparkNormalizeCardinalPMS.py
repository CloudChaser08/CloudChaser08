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
from spark.common.era.summary_v1 import schema as summary_schema
from spark.common.era.detail_v1 import schema as detail_schema
import spark.helpers.schema_enforcer as schema_enforcer

import logging

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path_prefix = file_utils.get_abs_path(
            script_path,  '../../../test/providers/cardinal_pms/era/resources/input/{}'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path,  '../../../test/providers/cardinal_pms/era/resources/payload/{}'
        ) + '/'
    elif airflow_test:
        input_path_prefix = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/out/{}/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/payload/{}/'
    else:
        input_path_prefix = 's3://salusv/incoming/era/cardinal_pms/{}/'
        matching_path = 's3://salusv/incoming/era/cardinal_pms/{}/'

    input_path_prefix = input_path_prefix.format(date_input.replace('-', '/'))
    matching_path     = matching_path.format(date_input.replace('-', '/'))

    external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
                    runner.sqlContext,
                    '66',
                    None,
                    'EARLIEST_VALID_SERVICE_DATE'
                )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    import load_records
    load_records.load(runner, input_path_prefix)

    # Normalize claim summary
    summary = runner.run_spark_script('summary_mapping.sql', return_output=True)
    logging.debug('Finished normalizing claim summary')

    # Normalize detail lines
    detail_line = runner.run_spark_script('detail_line_mapping.sql', return_output=True)
    logging.debug('Finished normalizing claim detail lines')

    import spark.providers.cardinal_pms.era.summary_privacy as summary_privacy

    # Postprocessing
    postprocessing = [
        {
            'data_type' : 'summary',
            'filename_template' : 'remit_claim_record_data_{}',
            'dataframe' : summary,
            'table' : 'era_summary_common_model',
            'model_script' : 'era/summary_v1.sql',
            'date_column' : 'clm_stmt_perd_end_dt',
            'service_cap' : ['clm_stmt_perd_start_dt', 'clm_stmt_perd_end_dt'],
            'privacy' : summary_privacy.apply_privacy,
            'schema' : summary_schema
        },
        {
            'data_type' : 'detail_line',
            'filename_template' : 'remit_service_line_record_data_{}',
            'dataframe' : detail_line,
            'table' : 'era_detail_line_common_model',
            'model_script' : 'era/detail_v1.sql',
            'date_column' : 'svc_ln_start_dt',
            'service_cap' : ['svc_ln_start_dt', 'svc_ln_end_dt'],
            'privacy' : lambda x: x,
            'schema' : detail_schema
        }
    ]

    hvm_historical = postprocessor.coalesce_dates(
                    runner.sqlContext,
                    '66',
                    date(1901, 1, 1),
                    'HVM_AVAILABLE_HISTORY_START_DATE',
                    'EARLIEST_VALID_SERVICE_DATE'
    )
    hvm_historical = datetime(hvm_historical.year, hvm_historical.month, hvm_historical.day)

    for conf in postprocessing:
        postprocessor.compose(
            schema_enforcer.apply_schema_func(conf['schema']),
            conf['privacy'],
            postprocessor.nullify,
            postprocessor.add_universal_columns(
                feed_id='66',
                vendor_id='42',
                filename=conf['filename_template'].format(date_obj.strftime('%Y%m%d')),
                model_version_number='01',
                **{
                    'record_id' : 'row_id',
                    'created' : 'crt_dt',
                    'data_set' : 'data_set_nm',
                    'data_feed' : 'hvm_vdr_feed_id',
                    'data_vendor' : 'hvm_vdr_id',
                    'model_version' : 'mdl_vrsn_num'
                }
            ),
            *[postprocessor.apply_date_cap(runner.sqlContext, c, max_date, '66', None, min_date)
                for c in conf['service_cap']]
        )(
            conf['dataframe']
        ).createTempView(conf['table'])
        logging.debug('Finished post-processing')
        
        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'era', conf['model_script'], '66',
                conf['table'], conf['date_column'], date_input,
                staging_subdir='{}/'.format(conf['data_type']),
                distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
                date_partition='part_mth', hvm_historical_date = hvm_historical
            )


def main(args):
    # init
    spark, sqlContext = init('Cardinal PMS')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/era/spark-output/'
    else:
        output_path = 's3://salusv/deliverable/cardinal_pms-0/' + args.date

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
