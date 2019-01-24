import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.file_utils as file_utils

TABLES = ['address', 'clinicpreference', 'dialysistraining', 'dialysistreatment',
          'facilityadmitdischarge', 'hospitalization', 'immunization', 'insurance',
          'labidlist', 'labpanelsdrawn', 'labresult', 'medication', 'medicationgroup',
          'modalitychangehistorycrownweb', 'nursinghomehistory', 'patientaccess',
          'patientaccess_examproc', 'patientaccess_otheraccessevent',
          'patientaccess_placedrecorded', 'patientaccess_removed', 'patientallergy',
          'patientcms2728', 'patientcomorbidityandtransplantstate', 'patientdata',
          'patientdiagcodes', 'patientdialysisprescription', 'patientdialysisrxhemo',
          'patientdialysisrxpd', 'patientdialysisrxpdexchanges', 'patientevent',
          'patientfluidweightmanagement', 'patientheighthistory', 'patientinfection',
          'patientinfection_laborganism', 'patientinfection_laborganismdrug',
          'patientinfection_labresultculture', 'patientinfection_medication',
          'patientinstabilityhistory', 'patientmasterscheduleheader',
          'patientmedadministered', 'patientmednotgiven', 'patientmedprescription',
          'patientstatushistory', 'problemlist', 'sodiumufprofile', 'stategeo',
          'zipgeo']

ADVANCE_DIRECTIVE_TABLE = 'advancedirective'

V2_START_DATE = '2018-11-01'

def run(spark, runner, date_input, test=False, airflow_test=False):
    tables = TABLES
    if date_input > V2_START_DATE:
        tables = tables + [ADVANCE_DIRECTIVE_TABLE]

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/visonex/custom/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/visonex/custom/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/visonex/emr/input/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/visonex/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/emr/visonex/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/visonex/{}/'.format(
            date_input.replace('-', '/')
        )

    payload_loader.load(runner, matching_path, ['claimId'])

    runner.run_spark_script('clean_visonex_tables_common.sql')
    runner.run_spark_script('load_transactions_common.sql', [
        ['input_path', input_path, False]
    ])

    if date_input > V2_START_DATE:
        runner.run_spark_script('clean_visonex_tables_v2.sql')
        runner.run_spark_script('load_transactions_v2.sql', [
            ['input_path', input_path, False]
        ])
    else:
        runner.run_spark_script('clean_visonex_tables_v1.sql')
        runner.run_spark_script('load_transactions_v1.sql', [
            ['input_path', input_path, False]
        ])

    # trim and nullify all incoming transactions tables
    for table in tables:
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.sql('select * from {}'.format(table))).createTempView(table)

    runner.run_spark_script('clean_up_visonex_common.sql')
    if date_input > V2_START_DATE:
        runner.run_spark_script('clean_up_visonex_v2.sql')
    else:
        runner.run_spark_script('clean_up_visonex_v1.sql')

    if not test:
        for table in tables:
            normalized_records_unloader.partition_custom(
                spark, runner, 'visonex', 'clean_' + table, None, date_input,
                partition_value=date_input[:7], staging_subdir='{}/'.format(table)
            )

def main(args):
    # init
    spark, sqlContext = init("Visonex EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/visonex/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/custom/2017-09-27/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
