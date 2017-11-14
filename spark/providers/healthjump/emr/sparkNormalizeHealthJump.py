import argparse
from datetime import datetime
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
            '/'.join(date_input.split('-')[:2])
        )
        matching_path = 's3a://salusv/matching/payload/emr/healthjump/{}/'.format(
            '/'.join(date_input.split('-')[:2])
        )

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    vendor_feed_id = '47'
    vendor_id = '217'

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    hvm_available_history_date = postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "HVM_AVAILABLE_HISTORY_START_DATE")
    earliest_valid_service_date = postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else datetime.date(1901, 1, 1)
    max_date = date_input

    runner.run_spark_script('../../../common/emr/diagnosis_common_model_v5.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/lab_order_common_model_v3.sql', [
        ['table_name', 'lab_order_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/procedure_common_model_v4.sql', [
        ['table_name', 'procedure_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model_v4.sql', [
        ['table_name', 'medication_common_model', False],
        ['properties', '', False]
    ])

    runner.run_spark_script('load_transactions.sql', [
        ['cpt_path', input_path + 'cpt/'],
        ['diag_path', input_path + 'diag/']
        ['loinc_path', input_path + 'loinc/']
        ['ndc_path', input_path + 'ndc/']
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
            lambda df: postprocessor.nullify(df, preprocess_func=lambda x: x.replace('X', ''))
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
            'script_name': 'diagnosis_common_model_v5.sql',
            'privacy_filter': diag_priv
        },
        {
            'table_name': 'procedure_common_model',
            'script_name': 'procedure_common_model_v3.sql',
            'privacy_filter': proc_priv
        },
        {
            'table_name': 'lab_order_common_model',
            'script_name': 'lab_order_common_model_v4.sql',
            'privacy_filter': lab_order_priv
        },
        {
            'table_name': 'medication_common_model',
            'script_name': 'medication_common_model_v4.sql',
            'privacy_filter': med_priv
        }
    ]
    for normalized_table in normalized_tables:
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id=vendor_feed_id,
                vendor_id=vendor_id,
                filename='record_data_HV_{}.txt.name.csv'.format(date_obj.strftime('%Y%m%d'))
            ),
            normalized_table['privacy_filter'].filter,
            postprocessor.apply_date_cap(runner.sqlContext, 'date_specimen', max_date, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE')
        )(
            runner.sqlContext.sql('select * from {}'.format(normalized_table['table_name']))
        ).createTempView(normalized_table['table_name'])

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', normalized_table['script_name'], 'healthjump',
                normalized_table['table_name'], 'date_specimen', date_input,
                hvm_historical_date=datetime(
                    hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
                )
            )


def main(args):

    # init spark
    spark, sqlContext = init("HealthJump")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/spark-output/'
    else:
        output_path = 's3a://salusv/warehouse/parquet/emr/2017-11-06/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
