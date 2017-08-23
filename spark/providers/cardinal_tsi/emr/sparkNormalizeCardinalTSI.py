import argparse
import time
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.helpers.privacy.emr import                   \
    diagnosis as priv_diagnosis,                        \
    medication as priv_medication

TODAY = time.strftime('%Y-%m-%d', time.localtime())


def run(spark, runner, date_input, test=False, airflow_test=False):

    # TODO: this isn't the way their files will be named...
    diag_setid = 'TSI_Diag_Sample_Raw.json'
    med_setid = 'TSI_Med_Sample_Raw.json'

    script_path = __file__

    if test:
        diagnosis_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/diagnosis/'
        ) + '/'
        medication_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/medication/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/matching/'
        ) + '/'
    elif airflow_test:
        diagnosis_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        medication_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/medication/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        diagnosis_input_path = 's3a://salusv/incoming/emr/cardinal/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        medication_input_path = 's3a://salusv/incoming/emr/cardinal/{}/medication/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/cardinal/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '1900-01-02'
    max_date = date_input

    runner.run_spark_script('../../../common/emr/diagnosis_common_model_v3.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model_v2.sql', [
        ['table_name', 'medication_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['personId'])

    runner.run_spark_script('load_transactions.sql', [
        ['diagnosis_input_path', diagnosis_input_path],
        ['medication_input_path', medication_input_path]
    ])

    transaction_tables = [
        'diagnosis_transactions', 'medication_transactions'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, lambda df: postprocessor.nullify(
                df,
                null_vals=['','NULL'],
                preprocess_func=lambda c: c.upper() if c else c
            )
        )(runner.sqlContext.sql('select * from {}'.format(table))).createTempView(table)

    runner.run_spark_script('normalize_diagnosis.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_medication.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    normalized_tables = [
        {
            'table_name': 'diagnosis_common_model',
            'script_name': 'emr/diagnosis_common_model_v3.sql',
            'data_type': 'diagnosis',
            'date_column': 'diag_dt',
            'setid': diag_setid,
            'privacy_filter': priv_diagnosis
        },
        {
            'table_name': 'medication_common_model',
            'script_name': 'emr/medication_common_model_v2.sql',
            'data_type': 'medication',
            'date_column': 'medctn_admin_dt',
            'setid': med_setid,
            'privacy_filter': priv_medication
        }
    ]

    for table in normalized_tables:
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id='31', vendor_id='42', filename=table['setid'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id'
            ),

            table['privacy_filter'].filter
        )(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createTempView(table['table_name'])

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', table['script_name'], 'cardinal_tsi',
                table['table_name'], table['date_column'], date_input,
                staging_subdir='{}/'.format(table['data_type']),
                distribution_key='row_id', provider_partition='hvm_vdr_feed_id',
                date_partition='prt_mnth'
            )


def main(args):
    # init
    spark, sqlContext = init("Cardinal TSI EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_tsi/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/2017-08-09/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
