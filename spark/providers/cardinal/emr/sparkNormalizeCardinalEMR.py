import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader

TODAY = time.strftime('%Y-%m-%d', time.localtime())


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'EMR.{}.zip'.format(date_obj.strftime('%m%d%Y'))

    script_path = __file__

    if test:
        demographics_input_path = '../../../test/providers/cardinal/emr/resources/input/demographics/'
        diagnosis_input_path = '../../../test/providers/cardinal/emr/resources/input/diagnosis/'
        encounter_input_path = '../../../test/providers/cardinal/emr/resources/input/encounter/'
        lab_input_path = '../../../test/providers/cardinal/emr/resources/input/lab/'
        dispense_input_path = '../../../test/providers/cardinal/emr/resources/input/dispense/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/matching/'
        ) + '/'
    elif airflow_test:
        demographics_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/demographics/'.format(
            date_input.replace('-', '/')
        )
        diagnosis_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        encounter_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/encounter/'.format(
            date_input.replace('-', '/')
        )
        lab_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/lab/'.format(
            date_input.replace('-', '/')
        )
        dispense_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/dispense/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        demographics_input_path = 's3a://salusv/incoming/emr/cardinal/{}/demographics/'.format(
            date_input.replace('-', '/')
        )
        diagnosis_input_path = 's3a://salusv/incoming/emr/cardinal/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        encounter_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/encounter/'.format(
            date_input.replace('-', '/')
        )
        lab_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/lab/'.format(
            date_input.replace('-', '/')
        )
        dispense_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/dispense/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/cardinal/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '0000-01-01'
    max_date = '9999-12-31'

    runner.run_spark_script('../../../common/emr/clinical_observation_common_model.sql', [
        ['table_name', 'clinical_observation_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/diagnosis_common_model.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/encounter_common_model.sql', [
        ['table_name', 'encounter_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/lab_result_common_model.sql', [
        ['table_name', 'lab_result_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model.sql', [
        ['table_name', 'medication_common_model', False],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/procedure_common_model.sql', [
        ['table_name', 'procedure_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('load_transactions.sql', [
        ['demographics_input_path', demographics_input_path],
        ['diagnosis_input_path', diagnosis_input_path],
        ['encounter_input_path', encounter_input_path],
        ['lab_input_path', lab_input_path],
        ['dispense_input_path', dispense_input_path]
    ])

    transaction_tables = [
        'demographics_transactions', 'diagnosis_transactions', 'encounter_transactions',
        'lab_transactions', 'dispense_transactions'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.sql('select * from {}'.format(table))).createTempView(table)

    runner.run_spark_script('normalize_encounter.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_diagnosis.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_procedure_enc.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_procedure_disp.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_lab_result.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_medication.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    runner.run_spark_script('normalize_clinical_observation.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    normalized_tables = [
        {
            'table_name': 'clinical_observation_common_model',
            'script_name': 'clinical_observation_common_model.sql',
            'data_type': 'clinical_observation'
        },
        {
            'table_name': 'diagnosis_common_model',
            'script_name': 'diagnosis_common_model.sql',
            'data_type': 'diagnosis'
        },
        {
            'table_name': 'encounter_common_model',
            'script_name': 'encounter_common_model.sql',
            'data_type': 'encounter'
        },
        {
            'table_name': 'medication_common_model',
            'script_name': 'medication_common_model.sql',
            'data_type': 'medication'
        },
        {
            'table_name': 'procedure_common_model',
            'script_name': 'procedure_common_model.sql',
            'data_type': 'procedure'
        }
    ]

    for table in normalized_tables:
        postprocessor.compose(
            postprocessor.nullify,
            postprocessor.add_universal_columns(
                feed_id='31', vendor_id='42', filename=setid,

                # rename defaults
                record_id='rec_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id'
            )

            # TODO: privacy filtering
        )(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createTempView(table['table_name'])

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', table['script_name'], 'cardinal',
                table, 'date_service', date_input, staging_subdir='{}/'.format(table['data_type'])
            )


def main(args):
    # init
    spark, sqlContext = init("Cardinal Rain Tree EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/2017-08-07/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
