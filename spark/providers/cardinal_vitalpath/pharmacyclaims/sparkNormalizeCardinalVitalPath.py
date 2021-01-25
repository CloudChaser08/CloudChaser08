#! /usr/bin/python
import argparse
from datetime import datetime, date
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims import schemas as pharma_schemas
import spark.helpers.file_utils as file_utils
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger

schema = pharma_schemas['schema_v6']
OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_vitalpath/pharmacyclaims/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/' + schema.output_directory


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    # NOTE: VERIFY THAT THIS IS TRUE BEFORE MERGING
    setid = 'cardinal_vital_path_.' + date_obj.strftime('%Y%m%d')

    script_path = __file__

    if test:
        patient_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_vitalpath/pharmacyclaims/resources/input/patient/'
        ) + '/'
        patient_matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_vitalpath/pharmacyclaims/resources/matching/patient/'
        ) + '/'
        medical_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_vitalpath/pharmacyclaims/resources/input/medical/'
        ) + '/'
        medical_matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_vitalpath/pharmacyclaims/resources/matching/medical/'
        ) + '/'
    elif airflow_test:
        patient_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_vitalpath/out/{}/patient/'.format(
            date_input.replace('-', '/')[:-3]
        )
        patient_matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_vitalpath/payload/{}/patient/'.format(
            date_input.replace('-', '/')[:-3]
        )
        medical_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_vitalpath/out/{}/medical/'.format(
            date_input.replace('-', '/')[:-3]
        )
        medical_matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_vitalpath/payload/{}/medical/'.format(
            date_input.replace('-', '/')[:-3]
        )
    else:
        patient_input_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/patient/'.format(
            date_input.replace('-', '/')[:-3]
        )
        patient_matching_path = \
            's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/patient_passthrough/'.format(
            date_input.replace('-', '/')[:-3]
        )
        medical_input_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/med/'.format(
            date_input.replace('-', '/')[:-3]
        )
        medical_matching_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/med_passthrough/'.format(
            date_input.replace('-', '/')[:-3]
        )

    if test:
        min_date = '1901-01-01'
    else:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

        min_date = postprocessor.get_gen_ref_date(
            runner.sqlContext,
            '30',
            'EARLIEST_VALID_SERVICE_DATE'
        ).isoformat()

    max_date = date_input

    payload_loader.load(runner, medical_matching_path, ['hvJoinKey', 'patientId'], 'cardinal_vitalpath_med_deid')
    payload_loader.load(runner, patient_matching_path, ['hvJoinKey', 'patientId'], 'cardinal_vitalpath_patient_deid')

    runner.run_spark_script('load_transactions.sql', [
        ['med_input_path', medical_input_path],
        ['patient_input_path', patient_input_path]
    ])

    med_df = runner.sqlContext.sql('select * from cardinal_vitalpath_med')
    postprocessor.trimmify(med_df).createTempView('cardinal_vitalpath_med')

    patient_df = runner.sqlContext.sql('select * from cardinal_vitalpath_patient')
    postprocessor.trimmify(patient_df).createTempView('cardinal_vitalpath_patient')

    normalized_output = runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)

    postprocessor.compose(
        lambda x: schema_enforcer.apply_schema(x, schema.schema_structure),
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='30', vendor_id='42', filename=setid),
        pharm_priv.filter
    )(
        normalized_output
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        hvm_historical = postprocessor.get_gen_ref_date(
            runner.sqlContext,
            '30',
            'HVM_AVAILABLE_HISTORY_START_DATE'
        )
        if hvm_historical is None:
            hvm_historical = postprocessor.get_gen_ref_date(
                runner.sqlContext,
                '30',
                'EARLIEST_VALID_SERVICE_DATE'
            )
        if hvm_historical is None:
            hvm_historical = date('1901', '1', '1')

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims/sql/pharmacyclaims_common_model_v6.sql',
            'cardinal_vitalpath', 'pharmacyclaims_common_model', 'date_service', date_input,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day)
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_VitalPath',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=patient_input_path,
            data_source_matching_path=medical_matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sqlContext = init("Cardinal_VitalPath")

    # initialize runner
    runner = Runner(sqlContext)

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
