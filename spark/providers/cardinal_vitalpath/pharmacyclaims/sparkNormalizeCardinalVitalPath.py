#! /usr/bin/python
import argparse
from datetime import datetime 
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

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
        patient_matching_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/patient_passthrough/'.format(
            date_input.replace('-', '/')[:-3]
        )
        medical_input_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/med/'.format(
            date_input.replace('-', '/')[:-3]
        )
        medical_matching_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_vitalpath/{}/med_passthrough/'.format(
            date_input.replace('-', '/')[:-3]
        )

    min_date = '2011-01-01'
    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v3.sql', [
        ['external', '', False],
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, medical_matching_path, ['hvJoinKey', 'patientid'], 'cardinal_vitalpath_med_deid')
    payload_loader.load(runner, patient_matching_path, ['hvJoinKey', 'patientid'], 'cardinal_vitalpath_patient_deid')

    runner.run_spark_script('load_transactions.sql', [
        ['med_input_path', medical_input_path],
        ['patient_input_path', patient_input_path]
    ])

    med_df = runner.sqlContext.sql('select * from cardinal_vitalpath_med')
    postprocessor.trimmify(med_df).createTempView('cardinal_vitalpath_med')

    patient_df = runner.sqlContext.sql('select * from cardinal_vitalpath_patient')
    postprocessor.trimmify(patient_df).createTempView('cardinal_vitalpath_patient')

    runner.run_spark_script('normalize.sql', []) 

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='30', vendor_id='42', filename=setid),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v3.sql', 'cardinal_pds',
            'pharmacyclaims_common_model_final', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("CardinalRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path      = 's3://salusv/testing/dewey/airflow/e2e/cardinal_vitalpath/pharmacyclaims/spark-output/'
    else:
        output_path      = 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'

    normalized_records_unloader.distcp(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

