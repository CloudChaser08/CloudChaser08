import argparse
import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.helpers.privacy.common import Transformer, TransformFunction
from spark.helpers.privacy.emr import                   \
    diagnosis as priv_diagnosis,                        \
    medication as priv_medication

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_tsi/emr/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/emr/2017-08-23/'


def run(spark, runner, date_input, test=False, airflow_test=False):

    # TODO: this isn't the way their files will be named
    diag_setid = 'TSI_Diag_Sample_Raw.json'
    med_setid = 'TSI_Med_Sample_Raw.json'

    script_path = __file__

    if test:
        diagnosis_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_tsi/emr/resources/input/diagnosis/'
        ) + '/'
        medication_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_tsi/emr/resources/input/medication/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_tsi/emr/resources/matching/'
        ) + '/'
    elif airflow_test:
        diagnosis_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_tsi/emr/out/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        medication_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_tsi/emr/out/{}/medication/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_tsi/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        diagnosis_input_path = 's3a://salusv/incoming/emr/cardinal_tsi/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        medication_input_path = 's3a://salusv/incoming/emr/cardinal_tsi/{}/medication/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/cardinal_tsi/{}/'.format(
            date_input.replace('-', '/')
        )

    runner.run_spark_script('../../../common/emr/diagnosis_common_model_v5.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['properties', '', False],
        ['additional_columns', '']
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model_v4.sql', [
        ['table_name', 'medication_common_model', False],
        ['properties', '', False],
        ['additional_columns', '']
    ])

    payload_loader.load(runner, matching_path, ['personId'])

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    runner.run_spark_script('load_transactions.sql', [
        ['diagnosis_input_path', diagnosis_input_path],
        ['medication_input_path', medication_input_path]
    ])

    transaction_tables = [
        'transactions_diagnosis', 'transactions_medication'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, lambda df: postprocessor.nullify(
                df,
                null_vals=['', 'NULL'],
                preprocess_func=lambda c: c.upper() if c else c
            )
        )(runner.sqlContext.sql('select * from {}'.format(table))).createTempView(table)

    runner.run_spark_script('normalize_diagnosis.sql')
    runner.run_spark_script('normalize_medication.sql')

    normalized_tables = [
        {
            'table_name': 'diagnosis_common_model',
            'script_name': 'emr/diagnosis_common_model_v5.sql',
            'data_type': 'diagnosis',
            'date_column': 'enc_dt',
            'setid': diag_setid,
            'privacy_filter': priv_diagnosis,
            'custom_transformer': Transformer(
                diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['diag_cd', 'diag_cd_qual', 'enc_dt'])
                ]
            ),
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', None)
            ]
        },
        {
            'table_name': 'medication_common_model',
            'script_name': 'emr/medication_common_model_v4.sql',
            'data_type': 'medication',
            'date_column': 'medctn_start_dt',
            'setid': med_setid,
            'privacy_filter': priv_medication,
            'date_caps': [
                ('medctn_start_dt', 'EARLIEST_VALID_SERVICE_DATE', None),
                ('medctn_end_dt', 'EARLIEST_VALID_SERVICE_DATE', None),
                ('medctn_last_rfll_dt', 'EARLIEST_VALID_SERVICE_DATE', lambda d: d - datetime.timedelta(year=1))
            ]
        }
    ]

    for table in normalized_tables:
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id='31', vendor_id='42', filename=table['setid'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),

            table['privacy_filter'].filter(runner.sqlContext, additional_transformer=table.get('custom_transformer')),
            *[
                postprocessor.apply_date_cap(
                    runner.sqlContext, date_col, date_input, '31', domain_name, date_function=date_fn)
                for (date_col, domain_name, date_fn) in table['date_caps']
            ]
        )(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createTempView(table['table_name'])

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', table['script_name'], '31',
                table['table_name'], table['date_column'], date_input,
                staging_subdir='{}/'.format(table['data_type']),
                distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
                date_partition='part_mth'
            )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_TSI_EMR',
            data_type=DataType.EMR,
            data_source_transaction_path=diagnosis_input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("Cardinal TSI EMR")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
