import argparse
import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.emr.diagnosis as diag_priv
import spark.helpers.privacy.emr.lab_order as lab_order_priv
import spark.helpers.privacy.emr.procedure as proc_priv
import spark.helpers.privacy.emr.medication as med_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3a://salusv/warehouse/parquet/emr/2017-08-23/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/healthjump/emr/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/healthjump/emr/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/emr/healthjump/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/healthjump/{}/'.format(
            date_input.replace('-', '/')
        )

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    vendor_feed_id = '47'
    vendor_id = '217'

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    hvm_available_history_date = \
        postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "HVM_AVAILABLE_HISTORY_START_DATE")
    earliest_valid_service_date = \
        postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else datetime.date(1901, 1, 1)
    max_date = date_input

    runner.run_spark_script('../../../common/emr/diagnosis_common_model_v5.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/lab_order_common_model_v3.sql', [
        ['table_name', 'lab_order_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/procedure_common_model_v4.sql', [
        ['table_name', 'procedure_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model_v4.sql', [
        ['table_name', 'medication_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])

    runner.run_spark_script('load_transactions.sql', [
        ['cpt_path', input_path + 'cpt/'],
        ['diag_path', input_path + 'diag/'],
        ['loinc_path', input_path + 'loinc/'],
        ['ndc_path', input_path + 'ndc/'],
        ['demographics_path', input_path + 'demographics/']
    ])

    transaction_tables = [
        'demographics_transactions', 'cpt_transactions', 'diagnosis_transactions',
        'loinc_transactions', 'ndc_transactions'
    ]

    # trim and remove nulls from raw input
    for transaction_table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify,
            lambda df: postprocessor.nullify(df, preprocess_func=lambda x: x.replace('X', '') if x else x)
        )(
            runner.sqlContext.sql('select * from {}'.format(transaction_table))
        ).createTempView(transaction_table)

    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'PCN', 'hvJoinKey'])

    runner.run_spark_script('normalize_diagnosis.sql')
    runner.run_spark_script('normalize_lab_order.sql')
    runner.run_spark_script('normalize_procedure.sql')
    runner.run_spark_script('normalize_medication.sql')

    normalized_tables = [
        {
            'table_name': 'diagnosis_common_model',
            'script_name': 'emr/diagnosis_common_model_v5.sql',
            'privacy_filter': diag_priv,
            'data_type': 'diagnosis',
            'min_cap_date_domain': 'EARLIEST_VALID_DIAGNOSIS_DATE',
            'date': 'diag_dt'
        },
        {
            'table_name': 'procedure_common_model',
            'script_name': 'emr/procedure_common_model_v4.sql',
            'privacy_filter': proc_priv,
            'data_type': 'procedure',
            'min_cap_date_domain': 'EARLIEST_VALID_SERVICE_DATE',
            'date': 'proc_dt'
        },
        {
            'table_name': 'lab_order_common_model',
            'script_name': 'emr/lab_order_common_model_v3.sql',
            'privacy_filter': lab_order_priv,
            'data_type': 'lab_order',
            'min_cap_date_domain': 'EARLIEST_VALID_SERVICE_DATE',
            'date': 'lab_ord_dt'
        },
        {
            'table_name': 'medication_common_model',
            'script_name': 'emr/medication_common_model_v4.sql',
            'privacy_filter': med_priv,
            'data_type': 'medication',
            'min_cap_date_domain': 'EARLIEST_VALID_SERVICE_DATE',
            'date': 'medctn_admin_dt'
        }
    ]
    for normalized_table in normalized_tables:
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id=vendor_feed_id,
                vendor_id=vendor_id,
                filename='record_data_HV_{}.txt.name.csv'.format(date_obj.strftime('%Y%m%d')),

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),
            normalized_table['privacy_filter'].filter(runner.sqlContext),
            postprocessor.apply_date_cap(
                runner.sqlContext, normalized_table['date'], max_date, vendor_feed_id,
                normalized_table['min_cap_date_domain']
            )
        )(
            runner.sqlContext.sql('select * from {}'.format(normalized_table['table_name']))
        ).createTempView(normalized_table['table_name'])

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', normalized_table['script_name'], '47',
                normalized_table['table_name'], normalized_table['date'], date_input,
                staging_subdir='{}/'.format(normalized_table['data_type']),
                distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
                date_partition='part_mth', hvm_historical_date=datetime(
                    hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
                )
            )
    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='HealthJump',
            data_type=DataType.EMR,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):

    # init spark
    spark, sql_context = init("HealthJump")

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
    args = parser.parse_args()
    main(args)
