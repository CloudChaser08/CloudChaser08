import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.file_utils as file_utils
import spark.common.utility.logger as logger
import pyspark.sql.functions as F

TABLES = \
    ['address', 'clinicpreference', 'dialysistraining', 'dialysistreatment',
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
        'zipgeo'
     ]

JOIN_TO_PATIENT_DATA = \
    ['address', 'dialysistraining', 'dialysistreatment',
        'facilityadmitdischarge', 'hospitalization', 'immunization', 'insurance',
        'labpanelsdrawn', 'labresult', 'nursinghomehistory',
        'patientaccess', 'patientaccess_examproc', 'patientaccess_otheraccessevent',
        'patientaccess_placedrecorded', 'patientaccess_removed', 'patientallergy',
        'patientcms2728', 'patientcomorbidityandtransplantstate',
        'patientdiagcodes', 'patientdialysisprescription',
        'patientevent', 'patientfluidweightmanagement', 'patientheighthistory',
        'patientinfection', 'patientinfection_laborganism', 'patientinfection_laborganismdrug',
        'patientinfection_labresultculture', 'patientinfection_medication',
        'patientinstabilityhistory', 'patientmasterscheduleheader',
        'patientmedadministered', 'patientmednotgiven', 'patientmedprescription',
        'patientstatushistory', 'problemlist'
     ]

ADVANCE_DIRECTIVE_TABLE = 'advancedirective'

V2_START_DATE = '2018-11-01'


def run(spark, runner, args, test=False, airflow_test=False):
    script_path = __file__
    tables = TABLES
    date_input = args.date
    version = 'v1'

    logger.log('Clean Up Visonex - ' + date_input)
    if date_input >= V2_START_DATE:
        tables = tables + [ADVANCE_DIRECTIVE_TABLE]
        version = 'v2'

    load_common_table = 'load_transactions_common.sql'
    load_version_tables = 'load_transactions_{}.sql'.format(version)
    clean_patient_data_table = 'clean_patient_data.sql'
    clean_common_tables = 'clean_up_visonex_common.sql'
    clean_version_tables = 'clean_up_visonex_{}.sql'.format(version)

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

    logger.log('Load Payloads')
    payload_loader.load(runner, matching_path, ['claimId'])

    logger.log('Create tables for source data')
    runner.run_spark_script(load_common_table, [['input_path', input_path, False]])
    logger.log(load_version_tables)
    runner.run_spark_script(load_version_tables, [['input_path', input_path, False]])

    if date_input >= V2_START_DATE:
        logger.log('Adding columns to account for mismatched schema')
        spark.table('patientdata') \
            .withColumn('resuscitationcode', F.lit(None)) \
            .withColumn('advdirresuscitationcode', F.lit(None)) \
            .withColumn('advdirpowerofatty', F.lit(None)) \
            .withColumn('advdirlivingwill', F.lit(None)) \
            .withColumn('advdirpatientdeclines', F.lit(None)) \
            .withColumn('advrevdate', F.lit(None)) \
            .createOrReplaceTempView('patientdata')

        spark.table('patientmasterscheduleheader') \
            .withColumn('patientidnumber2', F.lit('')) \
            .withColumn('patientstatus2', F.lit('')) \
            .createOrReplaceTempView('patientmasterscheduleheader')

    logger.log('Trimmify and nullify all tables')
    # trim and nullify all incoming transactions tables
    for table in tables:
        logger.log('Trimming and Nullifying table: ' + table)
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.sql('select * from {}'.format(table))).createOrReplaceTempView(table)

    logger.log('Clean the patient data table')
    runner.run_spark_script(clean_patient_data_table)

    logger.log('Add demographics info to all delivery tables except patientdata')
    query = '''
                SELECT  hvid, a.age as pat_age, UPPER(LEFT(a.sex, 1)) as pat_sex, b.*
                FROM clean_patientdata a
                INNER JOIN {table_name} b 
                        ON a.clinicorganizationidnumber = b.clinicorganizationidnumber 
                       AND a.patientidnumber = b.patientidnumber
                WHERE a.analyticrowidnumber=regexp_replace(lower(a.analyticrowidnumber),'[a-z]+','')
            '''
    for table in JOIN_TO_PATIENT_DATA:
        logger.log('Adding payload data to table: ' + table)
        if table != 'patientdata':
            spark.sql(query.format(table_name=table)).createOrReplaceTempView(table)

    logger.log('Clean all tables')
    runner.run_spark_script(clean_common_tables)
    if date_input >= V2_START_DATE:
        runner.run_spark_script(clean_version_tables)

    logger.log('Unload all tables')
    if not test:
        for table in tables:
            logger.log('Write table to hdfs: ' + table)
            normalized_records_unloader.partition_custom(
                spark, runner, 'visonex', 'clean_' + table, None, date_input,
                partition_value=date_input[:7], staging_subdir='{}/'.format(table)
            )


def main(args):
    # init
    spark, sqlContext = init("Visonex EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args, airflow_test=args.airflow_test)

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
